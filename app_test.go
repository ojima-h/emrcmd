package main

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"reflect"
	"testing"
)

/*
 * Mock EMR API
 */
type MockEMR struct {
	emriface.EMRAPI

	LastRunJobFlowInput *emr.RunJobFlowInput
	MockRunJobFlow      func(*emr.RunJobFlowInput) (*emr.RunJobFlowOutput, error)

	LastWaitUntilClusterRunningInput *emr.DescribeClusterInput
	MockWaitUntilClusterRunning      func(*emr.DescribeClusterInput) error

	LastListClustersPagesInput *emr.ListClustersInput
	MockListClustersPages      func(*emr.ListClustersInput, func(*emr.ListClustersOutput, bool) bool) error

	LastDescribeClusterInput *emr.DescribeClusterInput
	MockDescribeCluster      func(input *emr.DescribeClusterInput) (*emr.DescribeClusterOutput, error)

	LastListInstanceGroupsPagesInput *emr.ListInstanceGroupsInput
	MockListInstanceGroupsPages      func(*emr.ListInstanceGroupsInput, func(*emr.ListInstanceGroupsOutput, bool) bool) error

	LastAddInstanceGroupsInput *emr.AddInstanceGroupsInput
	MockAddInstanceGroups      func(*emr.AddInstanceGroupsInput) (*emr.AddInstanceGroupsOutput, error)

	LastModifyInstanceGroupsInput *emr.ModifyInstanceGroupsInput
	MockModifyInstanceGroups      func(*emr.ModifyInstanceGroupsInput) (*emr.ModifyInstanceGroupsOutput, error)

	LastTerminateJobFlowsInput *emr.TerminateJobFlowsInput
	MockTerminateJobFlows      func(*emr.TerminateJobFlowsInput) (*emr.TerminateJobFlowsOutput, error)
}

func (m *MockEMR) RunJobFlow(input *emr.RunJobFlowInput) (*emr.RunJobFlowOutput, error) {
	m.LastRunJobFlowInput = input
	if f := m.MockRunJobFlow; f != nil {
		return f(input)
	} else {
		return &emr.RunJobFlowOutput{
			JobFlowId: aws.String("j-00000000"),
		}, nil
	}
}

func (m *MockEMR) WaitUntilClusterRunning(input *emr.DescribeClusterInput) error {
	m.LastWaitUntilClusterRunningInput = input
	if f := m.MockWaitUntilClusterRunning; f != nil {
		return f(input)
	} else {
		return nil
	}
}

func (m *MockEMR) ListClustersPages(input *emr.ListClustersInput, fn func(*emr.ListClustersOutput, bool) bool) error {
	m.LastListClustersPagesInput = input
	if f := m.MockListClustersPages; f != nil {
		return f(input, fn)
	} else {
		fn(&emr.ListClustersOutput{
			Clusters: []*emr.ClusterSummary{
				{
					Id:   aws.String("j-00000000"),
					Name: aws.String("test"),
					NormalizedInstanceHours: aws.Int64(10),
					Status: &emr.ClusterStatus{
						State: aws.String(emr.ClusterStateWaiting),
					},
				},
			},
		}, false)
		return nil
	}
}

func (m *MockEMR) DescribeCluster(input *emr.DescribeClusterInput) (*emr.DescribeClusterOutput, error) {
	m.LastDescribeClusterInput = input
	if f := m.MockDescribeCluster; f != nil {
		return f(input)
	} else {
		return &emr.DescribeClusterOutput{
			Cluster: &emr.Cluster{
				MasterPublicDnsName: aws.String("master-public-dns-name"),
			},
		}, nil
	}
}

func (m *MockEMR) ListInstanceGroupsPages(input *emr.ListInstanceGroupsInput, fn func(*emr.ListInstanceGroupsOutput, bool) bool) error {
	m.LastListInstanceGroupsPagesInput = input
	if f := m.MockListInstanceGroupsPages; f != nil {
		return f(input, fn)
	} else {
		fn(&emr.ListInstanceGroupsOutput{
			InstanceGroups: []*emr.InstanceGroup{
				{
					Id:                     aws.String("ig-00000001"),
					Name:                   aws.String("master"),
					InstanceGroupType:      aws.String(emr.InstanceGroupTypeMaster),
					RequestedInstanceCount: aws.Int64(1),
					RunningInstanceCount:   aws.Int64(1),
				},
				{
					Id:                     aws.String("ig-00000002"),
					Name:                   aws.String("core"),
					InstanceGroupType:      aws.String(emr.InstanceGroupTypeCore),
					RequestedInstanceCount: aws.Int64(5),
					RunningInstanceCount:   aws.Int64(2),
				},
			},
		}, false)
		return nil
	}
}

func (m *MockEMR) AddInstanceGroups(input *emr.AddInstanceGroupsInput) (*emr.AddInstanceGroupsOutput, error) {
	m.LastAddInstanceGroupsInput = input
	if f := m.MockAddInstanceGroups; f != nil {
		return f(input)
	} else {
		return &emr.AddInstanceGroupsOutput{}, nil
	}
}

func (m *MockEMR) ModifyInstanceGroups(input *emr.ModifyInstanceGroupsInput) (*emr.ModifyInstanceGroupsOutput, error) {
	m.LastModifyInstanceGroupsInput = input
	if f := m.MockModifyInstanceGroups; f != nil {
		return f(input)
	} else {
		return &emr.ModifyInstanceGroupsOutput{}, nil
	}
}

func (m *MockEMR) TerminateJobFlows(input *emr.TerminateJobFlowsInput) (*emr.TerminateJobFlowsOutput, error) {
	m.LastTerminateJobFlowsInput = input
	if f := m.MockTerminateJobFlows; f != nil {
		return f(input)
	} else {
		return &emr.TerminateJobFlowsOutput{}, nil
	}
}

/*
 * Mock App
 */
type MockApp struct {
	App
	EMRAPI    *MockEMR
	Stdout    *bytes.Buffer
	Stderr    *bytes.Buffer
	OpHandler *MockOperationHandle
}

type MockOperationHandle struct {
	LastGetInput string
	MockGet      func(string) ([]byte, error)

	LastExecInput []string
}

func (m *MockOperationHandle) HttpGet(url string) ([]byte, error) {
	m.LastGetInput = url
	if m.MockGet != nil {
		return m.MockGet(url)
	} else {
		resp := `{
			"clusterMetrics": {
				"containersAllocated":100,
				"containersPending":80,
				"allocatedMB":6000,
				"totalMB":10000
			}
		}`
		return []byte(resp), nil

	}
}

func (m *MockOperationHandle) Exec(args []string) error {
	m.LastExecInput = args
	return nil
}

func NewMockApp() *MockApp {
	m := &MockEMR{}
	o := bytes.NewBufferString("")
	e := bytes.NewBufferString("")
	h := &MockOperationHandle{}

	return &MockApp{
		App: App{
			EMRAPI:    m,
			Stdout:    o,
			Stderr:    e,
			OpHandler: h,
		},
		EMRAPI:    m,
		Stdout:    o,
		Stderr:    e,
		OpHandler: h,
	}
}

/*
 * Test Start
 */
func TestStart(t *testing.T) {
	a := NewMockApp()

	err := a.Start(&AppStartOptions{
		Name:     "test-cluster",
		Filename: "./cluster-sample.yml",
	})
	if err != nil {
		t.Fatalf("Start command expected to success but failed with %s", err.Error())
	}

	name := aws.StringValue(a.EMRAPI.LastRunJobFlowInput.Name)
	if "test-cluster" != name {
		t.Errorf("test-cluster expected but %s", name)
	}

	ig := a.EMRAPI.LastRunJobFlowInput.Instances.InstanceGroups
	if n := len(ig); 2 != n {
		t.Errorf("InstanceGroups length expeced 2 but %d", n)
	}

	size := aws.Int64Value(ig[1].InstanceCount)
	if 1 != size {
		t.Errorf("Core InstanceCount expected 1 but %d", size)
	}

	id := aws.StringValue(a.EMRAPI.LastWaitUntilClusterRunningInput.ClusterId)
	if "j-00000000" != id {
		t.Errorf("j-00000000 expected but %s", id)
	}
}

func TestStartWithSize(t *testing.T) {
	a := NewMockApp()

	err := a.Start(&AppStartOptions{
		Name:     "test-cluster",
		Filename: "./cluster-sample.yml",
		Vars:     map[string]string{"core": "0"},
	})
	if err != nil {
		t.Fatalf("Start command expected to success but failed with %s", err.Error())
	}

	name := aws.StringValue(a.EMRAPI.LastRunJobFlowInput.Name)
	if "test-cluster" != name {
		t.Errorf("test-cluster expected but %s", name)
	}

	ig := a.EMRAPI.LastRunJobFlowInput.Instances.InstanceGroups
	if n := len(ig); 1 != n {
		t.Errorf("InstanceGroups length expeced 1 but %d", n)
	}

	id := aws.StringValue(a.EMRAPI.LastWaitUntilClusterRunningInput.ClusterId)
	if "j-00000000" != id {
		t.Errorf("j-00000000 expected but %s", id)
	}
}

/*
 * Test List
 */
func TestList(t *testing.T) {
	a := NewMockApp()

	err := a.List(&AppListOptions{
		NoMaster:      true,
		NoMetrics:     true,
		NoClusterSize: true,
	})
	if err != nil {
		t.Fatalf("List command expected to success but failed with %s", err.Error())
	}

	sts := aws.StringValueSlice(a.EMRAPI.LastListClustersPagesInput.ClusterStates)
	if !reflect.DeepEqual(ClusterStateActive, sts) {
		t.Errorf("Only active clusters are expected to fetch")
	}
}

func TestListDetail(t *testing.T) {
	a := NewMockApp()

	err := a.List(&AppListOptions{})
	if err != nil {
		t.Fatalf("List command expected to success but failed with %s", err.Error())
	}

	exp1 := "http://master-public-dns-name:8088/ws/v1/cluster/metrics"
	if u := a.OpHandler.LastGetInput; exp1 != u {
		t.Errorf("'%s' expected but got '%s'", exp1, u)
	}

	exp2 :=
		`test  WAITING  j-00000000  10
  Master: master-public-dns-name
  MemoryUsed:  60%  |  ContainersRunning: 100  |  ContainersPending: 80
  Nodes:
    master: 1
    core: 2(5)

`
	out := a.Stdout.String()
	if exp2 != out {
		t.Errorf("'%s' expected but '%s'", exp2, out)
	}
}

/*
 * Test Resize
 */
func TestResizeNew(t *testing.T) {
	a := NewMockApp()

	err := a.Resize(&AppResizeOptions{
		Name:              "test",
		InstanceGroupName: "task",
		Size:              2,
		Filename:          "./cluster-sample.yml",
	})
	if err != nil {
		t.Fatalf("Resize command expected to success but failed with %s", err.Error())
	}

	input := a.EMRAPI.LastAddInstanceGroupsInput
	if input == nil {
		t.Fatalf("AddInstanceGroup API is expected to be caleld but not")
	}

	exp := &emr.AddInstanceGroupsInput{
		JobFlowId: aws.String("j-00000000"),
		InstanceGroups: []*emr.InstanceGroupConfig{
			{
				Name:          aws.String("task"),
				InstanceType:  aws.String("m3.xlarge"),
				InstanceRole:  aws.String("TASK"),
				InstanceCount: aws.Int64(2),
				Market:        aws.String("SPOT"),
				BidPrice:      aws.String("0.5"),
			},
		},
	}
	if !reflect.DeepEqual(exp, input) {
		t.Errorf("%s expected but got %s", exp, input)
	}

	if a.EMRAPI.LastModifyInstanceGroupsInput != nil {
		t.Fatalf("ModifyInstanceGroup API is expected not to be caleld but called")
	}
}

func TestResizeExisting(t *testing.T) {
	a := NewMockApp()

	err := a.Resize(&AppResizeOptions{
		Name:              "test",
		InstanceGroupName: "core",
		Size:              4,
		Filename:          "./cluster-sample.yml",
	})
	if err != nil {
		t.Fatalf("Resize command expected to success but failed with %s", err.Error())
	}

	input := a.EMRAPI.LastModifyInstanceGroupsInput
	if input == nil {
		t.Fatalf("ModifyInstanceGroup API is expected to be caleld but not")
	}

	exp := &emr.ModifyInstanceGroupsInput{
		ClusterId: aws.String("j-00000000"),
		InstanceGroups: []*emr.InstanceGroupModifyConfig{
			{
				InstanceGroupId: aws.String("ig-00000002"),
				InstanceCount:   aws.Int64(4),
			},
		},
	}
	if !reflect.DeepEqual(exp, input) {
		t.Errorf("%s expected but got %s", exp, input)
	}

	if a.EMRAPI.LastAddInstanceGroupsInput != nil {
		t.Fatalf("AddInstanceGroup API is expected not to be caleld but called")
	}
}

/*
 * Test Terminate
 */
func TestTerminate(t *testing.T) {
	a := NewMockApp()

	err := a.Terminate("test")
	if err != nil {
		t.Fatalf("Termiante command expected to success but failed with %s", err.Error())
	}

	input := a.EMRAPI.LastTerminateJobFlowsInput
	if input == nil {
		t.Fatalf("TermianteJobFlows API is expected to be caleld but not")
	}

	ids := input.JobFlowIds
	if n := len(ids); 1 != n {
		t.Fatalf("%d expected but got %d", 1, n)
	}

	if id := aws.StringValue(ids[0]); "j-00000000" != id {
		t.Fatalf("'%s' expected but got '%s'", "j-00000000", id)
	}
}

/*
 * Test SSH
 */
func TestSSH(t *testing.T) {
	a := NewMockApp()

	err := a.SSH(&AppSSHOptions{
		Name: "test",
		Options: map[string]string{
			"ServerAliveInterval": "10",
		},
	})
	if err != nil {
		t.Fatalf("SSH command expected to success but failed with %s", err.Error())
	}

	input := a.OpHandler.LastExecInput
	exp := []string{
		"ssh",
		"-o", "ServerAliveInterval=10",
		"master-public-dns-name",
	}

	if !reflect.DeepEqual(exp, input) {
		t.Errorf("%s expected but got %s", exp, input)
	}
}

/*
 * Test SCP
 */
func TestSCP(t *testing.T) {
	a := NewMockApp()

	err := a.SCP(&AppSCPOptions{
		Name: "test",
		Args: []string{"@:*.q", "."},
	})
	if err != nil {
		t.Fatalf("SSH command expected to success but failed with %s", err.Error())
	}

	input := a.OpHandler.LastExecInput
	exp := []string{"scp", "hadoop@master-public-dns-name:*.q", "."}

	if !reflect.DeepEqual(exp, input) {
		t.Errorf("%s expected but got %s", exp, input)
	}
}
