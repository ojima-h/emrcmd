package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"gopkg.in/urfave/cli.v1"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/template"
	"time"
)

type App struct {
	EMRAPI    emriface.EMRAPI
	Stdout    io.Writer
	Stderr    io.Writer
	OpHandler OperationHandler
}

func NewApp() *App {
	sess := session.Must(session.NewSession())
	return &App{
		EMRAPI:    emr.New(sess),
		Stdout:    os.Stdout,
		Stderr:    cli.ErrWriter,
		OpHandler: &OperationHandle{},
	}
}

type OperationHandler interface {
	HttpGet(uri string) ([]byte, error)
	Exec(args []string) error
}

type OperationHandle struct{}

func (*OperationHandle) HttpGet(url string) ([]byte, error) {
	client := http.Client{Timeout: time.Duration(1) * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func (c *OperationHandle) Exec(args []string) error {
	cmd, err := exec.LookPath(args[0])
	if err != nil {
		return err
	}
	env := os.Environ()
	return syscall.Exec(cmd, args, env)
}

var (
	ClusterStateAll = []string{
		emr.ClusterStateStarting,
		emr.ClusterStateBootstrapping,
		emr.ClusterStateRunning,
		emr.ClusterStateWaiting,
		emr.ClusterStateTerminating,
		emr.ClusterStateTerminated,
		emr.ClusterStateTerminatedWithErrors,
	}
	ClusterStateActive = []string{
		emr.ClusterStateStarting,
		emr.ClusterStateBootstrapping,
		emr.ClusterStateRunning,
		emr.ClusterStateWaiting,
	}
)

/*
 * Helper Functions
 */
func loadClusterConfig(filename string, name string, vars map[string]string) (*emr.RunJobFlowInput, error) {
	funcMap := template.FuncMap{
		"name": func() string { return name },
		"lookup": func(key string, defval interface{}) interface{} {
			val, ok := vars[key]
			if ok {
				return val
			}

			val, ok = os.LookupEnv("EMR_VAR_" + strings.ToUpper(key))
			if ok {
				return val
			}

			return defval
		},
		"env": os.Getenv,
	}

	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	t, err := template.New("cluster").Funcs(funcMap).Parse(string(dat))
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBufferString("")
	err = t.Execute(buf, vars)
	if err != nil {
		return nil, err
	}

	ret := emr.RunJobFlowInput{}
	err = yaml.Unmarshal(buf.Bytes(), &ret)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func (s *App) FindByName(name string) (*emr.ClusterSummary, error) {
	in := emr.ListClustersInput{
		ClusterStates: aws.StringSlice(ClusterStateActive),
	}

	var ret *emr.ClusterSummary
	err := s.EMRAPI.ListClustersPages(&in, func(out *emr.ListClustersOutput, b bool) bool {
		for _, c := range out.Clusters {
			if aws.StringValue(c.Name) == name {
				ret = c
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, fmt.Errorf("cluster %s is not found", name)
	}

	return ret, nil
}

func (s *App) FindInstanceGroupByName(id string, name string) (*emr.InstanceGroup, error) {
	in := emr.ListInstanceGroupsInput{ClusterId: aws.String(id)}

	var ret *emr.InstanceGroup
	err := s.EMRAPI.ListInstanceGroupsPages(&in, func(out *emr.ListInstanceGroupsOutput, b bool) bool {
		for _, ig := range out.InstanceGroups {
			if aws.StringValue(ig.Name) == name {
				ret = ig
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *App) GetMaster(id string) (string, error) {
	in := emr.DescribeClusterInput{ClusterId: aws.String(id)}
	out, err := s.EMRAPI.DescribeCluster(&in)
	if err != nil {
		return "", err
	}
	return aws.StringValue(out.Cluster.MasterPublicDnsName), nil
}

/*
 * Start new cluster
 */
type AppStartOptions struct {
	Name     string
	Vars     map[string]string
	Filename string
	DryRun   bool
}

func (s *App) Start(o *AppStartOptions) error {
	config, err := loadClusterConfigForStart(o.Filename, o.Name, o.Vars)
	if err != nil {
		return err
	}

	if o.DryRun {
		fmt.Fprintln(s.Stderr, "Start cluster with:")
		fmt.Fprintln(s.Stdout, config)
		return nil
	}

	fmt.Fprintf(s.Stderr, "starting cluster %s ...\n", o.Name)

	out, err := s.EMRAPI.RunJobFlow(config)
	if err != nil {
		return err
	}

	err = s.EMRAPI.WaitUntilClusterRunning(&emr.DescribeClusterInput{ClusterId: out.JobFlowId})
	if err != nil {
		return err
	}

	return nil
}

func loadClusterConfigForStart(filename string, name string, vars map[string]string) (*emr.RunJobFlowInput, error) {
	config, err := loadClusterConfig(filename, name, vars)
	if err != nil {
		return nil, err
	}

	igs := config.Instances.InstanceGroups
	var newIgs []*emr.InstanceGroupConfig
	for _, ig := range igs {
		if aws.Int64Value(ig.InstanceCount) > 0 {
			newIgs = append(newIgs, ig)
		}
	}
	config.Instances.InstanceGroups = newIgs

	return config, nil
}

/*
 * List clusters
 */
type AppListOptions struct {
	All           bool
	NoMaster      bool
	NoMetrics     bool
	NoClusterSize bool
	Limit         int
}

func (s *App) List(o *AppListOptions) (err error) {
	var states []string
	if o.All {
		states = ClusterStateAll
	} else {
		states = ClusterStateActive
	}

	i := 0
	e := s.EMRAPI.ListClustersPages(&emr.ListClustersInput{ClusterStates: aws.StringSlice(states)}, func(out *emr.ListClustersOutput, b bool) bool {
		for _, cls := range out.Clusters {
			err = s.printClusterInfo(o, cls)
			if err != nil {
				return false
			}
			if i += 1; i >= o.Limit {
				return false
			}
		}
		return true
	})
	if e != nil && err == nil {
		err = e
	}
	return
}

func (s *App) printClusterInfo(o *AppListOptions, cluster *emr.ClusterSummary) error {
	id := aws.StringValue(cluster.Id)
	name := aws.StringValue(cluster.Name)
	state := aws.StringValue(cluster.Status.State)
	hour := strconv.FormatInt(aws.Int64Value(cluster.NormalizedInstanceHours), 10)
	fmt.Fprintln(s.Stdout, strings.Join([]string{name, state, id, hour}, "  "))

	// Master
	var master string
	var err error
	if !o.NoMaster {
		master, err = s.GetMaster(id)
		if err != nil {
			return err
		}
		fmt.Fprintln(s.Stdout, "  Master: "+master)
	}

	// Cluster Metrics
	if !o.NoMetrics && master != "" && (state == emr.ClusterStateRunning || state == emr.ClusterStateWaiting) {
		err := s.printClusterMetrics(master)
		if err != nil {
			return err
		}
	}

	// Cluster Size
	if !o.NoClusterSize {
		err := s.printClusterSize(id)
		if err != nil {
			return err
		}
	}

	if !o.NoMaster || !o.NoMetrics || o.NoClusterSize {
		fmt.Fprintln(s.Stdout)
	}

	return nil
}

func (s *App) printClusterMetrics(master string) error {
	uri := fmt.Sprintf("http://%s:8088/ws/v1/cluster/metrics", master)
	m, err := s.getClusterMetrics(uri)
	if err != nil {
		return err
	}

	fmt.Fprintf(
		s.Stdout,
		"  MemoryUsed:  %d%%"+
			"  |  ContainersRunning: %d"+
			"  |  ContainersPending: %d"+
			"\n",
		m.MemoryUsed,
		m.ContainersAllocated,
		m.ContainersPending)

	return nil
}

type ClusterMetrics struct {
	ContainersAllocated int   `json:"containersAllocated"`
	ContainersPending   int   `json:"containersPending"`
	AllocatedMB         int64 `json:"allocatedMB"`
	TotalMB             int64 `json:"totalMB"`
	MemoryUsed          int
}

type ClusterMetricsBuffer struct {
	ClusterMetrics ClusterMetrics `json:"clusterMetrics"`
}

func (s *App) getClusterMetrics(url string) (*ClusterMetrics, error) {
	buf, err := s.OpHandler.HttpGet(url)
	if err != nil {
		return nil, err
	}

	dat := ClusterMetricsBuffer{}
	err = json.Unmarshal(buf, &dat)
	if err != nil {
		return nil, err
	}

	ret := dat.ClusterMetrics
	ret.MemoryUsed = int(float64(ret.AllocatedMB) / float64(ret.TotalMB) * 100)

	return &ret, nil
}

func (s *App) printClusterSize(id string) error {
	fmt.Fprintln(s.Stdout, "  Nodes:")

	in := emr.ListInstanceGroupsInput{ClusterId: aws.String(id)}
	var igs []*emr.InstanceGroup
	err := s.EMRAPI.ListInstanceGroupsPages(&in, func(out *emr.ListInstanceGroupsOutput, b bool) bool {
		for _, ig := range out.InstanceGroups {
			igs = append(igs, ig)
		}
		return true
	})
	if err != nil {
		return err
	}

	sort.Slice(igs, func(i, j int) bool {
		return encodeInstanceGroupType(igs[i].InstanceGroupType) < encodeInstanceGroupType(igs[j].InstanceGroupType)
	})

	for _, ig := range igs {
		var name = aws.StringValue(ig.Name)
		var run = aws.Int64Value(ig.RunningInstanceCount)
		var req = aws.Int64Value(ig.RequestedInstanceCount)
		if run == req {
			fmt.Fprintf(s.Stdout, "    %s: %d\n", name, run)
		} else {
			fmt.Fprintf(s.Stdout, "    %s: %d(%d)\n", name, run, req)
		}
	}

	return nil
}

func encodeInstanceGroupType(t *string) int {
	switch aws.StringValue(t) {
	case emr.InstanceGroupTypeMaster:
		return 0
	case emr.InstanceGroupTypeCore:
		return 1
	case emr.InstanceGroupTypeTask:
		return 2
	default:
		return 9
	}
}

/*
 * Resize cluster
 */
type AppResizeOptions struct {
	Name              string
	InstanceGroupName string
	Size              int
	Vars              map[string]string
	Filename          string
	DryRun            bool
}

func (s *App) Resize(o *AppResizeOptions) error {
	config, err := loadClusterConfigForResize(o.InstanceGroupName, o.Size, o.Filename, o.Name, o.Vars)
	if err != nil {
		return err
	}

	if o.DryRun {
		fmt.Fprintln(s.Stderr, "Resize cluster with:")
		fmt.Fprintln(s.Stdout, config)
		return nil
	}

	c, err := s.FindByName(o.Name)
	if err != nil {
		return err
	}

	ig, err := s.FindInstanceGroupByName(aws.StringValue(c.Id), o.InstanceGroupName)
	if err != nil {
		return err
	}

	if ig == nil {
		// Create new instance group
		in := emr.AddInstanceGroupsInput{
			JobFlowId:      c.Id,
			InstanceGroups: []*emr.InstanceGroupConfig{config},
		}
		_, err := s.EMRAPI.AddInstanceGroups(&in)
		if err != nil {
			return err
		}
	} else {
		// Update existing instance group size
		in := emr.ModifyInstanceGroupsInput{
			ClusterId: c.Id,
			InstanceGroups: []*emr.InstanceGroupModifyConfig{
				{
					InstanceGroupId: ig.Id,
					InstanceCount:   config.InstanceCount,
				},
			},
		}
		_, err := s.EMRAPI.ModifyInstanceGroups(&in)
		if err != nil {
			return err
		}
	}

	fmt.Fprintf(s.Stderr, "resizing %s to %d...\n", o.InstanceGroupName, o.Size)
	return nil
}

func loadClusterConfigForResize(instanceGroupName string, size int, filename string, name string, vars map[string]string) (*emr.InstanceGroupConfig, error) {
	config, err := loadClusterConfig(filename, name, vars)
	if err != nil {
		return nil, err
	}

	for _, ig := range config.Instances.InstanceGroups {
		if aws.StringValue(ig.Name) == instanceGroupName {
			ig.InstanceCount = aws.Int64(int64(size))
			return ig, nil
		}
	}

	return nil, fmt.Errorf("instance group %s is not found in configuration", instanceGroupName)
}

/*
 * Terminate cluster
 */
func (s *App) Terminate(name string) error {
	c, err := s.FindByName(name)
	if err != nil {
		return err
	}

	in := emr.TerminateJobFlowsInput{JobFlowIds: []*string{c.Id}}
	_, err = s.EMRAPI.TerminateJobFlows(&in)
	return err
}

/*
 * SSH cluster
 */
type AppSSHOptions struct {
	Name         string
	Args         []string
	IdentityFile string
	Options      map[string]string
	Debug        bool
}

func (s *App) SSH(o *AppSSHOptions) error {
	c, err := s.FindByName(o.Name)
	if err != nil {
		return err
	}

	master, err := s.GetMaster(aws.StringValue(c.Id))
	if err != nil {
		return err
	}

	args := []string{"ssh"}

	if o.IdentityFile != "" {
		args = append(args, "-i", o.IdentityFile)
	}
	for k, v := range o.Options {
		if v == "" {
			args = append(args, "-o", k)
		} else {
			args = append(args, "-o", k+"="+v)
		}
	}
	args = append(args, master)
	for _, v := range o.Args {
		args = append(args, v)
	}

	if o.Debug {
		fmt.Fprintln(s.Stderr, strings.Join(args, " "))
	}

	return s.OpHandler.Exec(args)
}

/*
 * SCP cluster
 */
type AppSCPOptions struct {
	Name         string
	Args         []string
	IdentityFile string
	Options      map[string]string
	Debug        bool
}

func (s *App) SCP(o *AppSCPOptions) error {
	c, err := s.FindByName(o.Name)
	if err != nil {
		return err
	}

	master, err := s.GetMaster(aws.StringValue(c.Id))
	if err != nil {
		return err
	}

	args := []string{"scp"}

	if o.IdentityFile != "" {
		args = append(args, "-i", o.IdentityFile)
	}
	for k, v := range o.Options {
		if v == "" {
			args = append(args, "-o", k)
		} else {
			args = append(args, "-o", k+"="+v)
		}
	}

	for _, v := range o.Args {
		rep := strings.Replace(v, "@:", "hadoop@"+master+":", -1)
		args = append(args, rep)
	}

	if o.Debug {
		fmt.Fprintln(s.Stderr, strings.Join(args, " "))
	}

	return s.OpHandler.Exec(args)
}

/*
 * SHELL command
 */
func (s *App) Shell(name string, args []string) error {
	c, err := s.FindByName(name)
	if err != nil {
		return err
	}

	master, err := s.GetMaster(aws.StringValue(c.Id))
	if err != nil {
		return nil
	}

	if len(args) == 0 {
		fmt.Fprintln(s.Stdout, "export EMR_MASTER="+master)
	} else {
		os.Setenv("EMR_MASTER", master)
		return s.OpHandler.Exec(args)
	}
	return nil
}
