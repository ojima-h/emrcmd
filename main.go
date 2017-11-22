package main

import (
	"fmt"
	"gopkg.in/urfave/cli.v1"
	"os"
	"path"
	"strconv"
	"strings"
)

func main() {
	BuildCLI(NewApp()).Run(os.Args)
}

func BuildCLI(a *App) *cli.App {
	app := cli.NewApp()
	app.Usage = "An EMR utility command"
	app.Commands = []cli.Command{
		{
			Name:      "start",
			Aliases:   []string{"up"},
			Usage:     "start new EMR cluster",
			ArgsUsage: "NAME [KEY=VAL ...]",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "filename, f",
					Value:  path.Join(os.Getenv("HOME"), ".emrcmd-cluster.yml"),
					EnvVar: "EMR_CLUSTER_CONFIG_FILE",
				},
				cli.BoolFlag{
					Name: "dryrun, n",
				},
			},
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 1, -1)

				args := c.Args()
				name := args[0]
				vars := parseVariables(args[1:])
				vars["name"] = name

				err := a.Start(&AppStartOptions{
					Name:     name,
					Vars:     vars,
					Filename: c.String("filename"),
					DryRun:   c.Bool("dryrun"),
				})

				if err != nil {
					return cli.NewExitError(err, 1)
				}

				return nil
			},
		},

		{
			Name:    "list",
			Aliases: []string{"ls"},
			Usage:   "list EMR clusters",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name: "all, a",
				},
				cli.BoolFlag{
					Name: "simple, s",
				},
				cli.BoolFlag{
					Name: "no-master, U",
				},
				cli.BoolFlag{
					Name: "no-metrics, M",
				},
				cli.BoolFlag{
					Name: "no-size, S",
				},
				cli.IntFlag{
					Name:  "limit, n",
					Value: 10,
				},
			},
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 0, 0)

				b := c.Bool("simple")
				err := a.List(&AppListOptions{
					All:           c.Bool("all"),
					NoMaster:      b || c.Bool("no-master"),
					NoMetrics:     b || c.Bool("no-metrics"),
					NoClusterSize: b || c.Bool("no-size"),
					Limit:         c.Int("limit"),
				})

				if err != nil {
					return cli.NewExitError(err, 1)
				}

				return nil
			},
		},
		{
			Name:      "resize",
			Usage:     "resize an EMR instance group",
			ArgsUsage: "NAME INSTANCE_GROUP_NAME SIZE [KEY=VAL ...]",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "filename, f",
					Value:  path.Join(os.Getenv("HOME"), ".emrcmd-cluster.yml"),
					EnvVar: "EMR_CLUSTER_CONFIG_FILE",
				},
				cli.BoolFlag{
					Name: "dryrun, n",
				},
			},
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 3, -1)

				name := c.Args().Get(0)
				instanceGroupName := c.Args().Get(1)
				size, err := strconv.Atoi(c.Args().Get(2))
				if err != nil {
					return cli.NewExitError(err, 1)
				}
				vars := parseVariables(c.Args()[3:])
				vars["name"] = name

				err = a.Resize(&AppResizeOptions{
					Name:              name,
					InstanceGroupName: instanceGroupName,
					Size:              size,
					Vars:              vars,
					Filename:          c.String("filename"),
					DryRun:            c.Bool("dryrun"),
				})

				if err != nil {
					return cli.NewExitError(err, 1)
				}

				return nil
			},
		},
		{
			Name:      "terminate",
			Aliases:   []string{"rm", "down"},
			Usage:     "terminate EMR cluster",
			ArgsUsage: "NAME",
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 1, 1)

				name := c.Args().Get(0)

				err := a.Terminate(name)
				if err != nil {
					return cli.NewExitError(err, 1)
				}

				return nil
			},
		},
		{
			Name:      "ssh",
			Usage:     "ssh to EMR cluster",
			ArgsUsage: "NAME [ARGS]",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "i",
					EnvVar: "EMR_SSH_IDENTITY_FILE",
				},
				cli.StringSliceFlag{
					Name:   "o",
					EnvVar: "ENV_SSH_OPTIONS",
					Value: &cli.StringSlice{
						"ServerAliveInterval=10",
						"StrictHostKeyChecking=no",
						"UserKnownHostsFile=/dev/null",
					},
					Usage: "SSH options in KEY=VAL format",
				},
				cli.BoolFlag{
					Name: "debug, d",
				},
			},
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 1, -1)

				name := c.Args().Get(0)
				args := c.Args()[1:]

				err := a.SSH(&AppSSHOptions{
					Name:         name,
					Args:         args,
					IdentityFile: c.String("i"),
					Options:      parseVariables(c.StringSlice("o")),
					Debug:        c.Bool("debug"),
				})
				if err != nil {
					return cli.NewExitError(err, 1)
				}

				return nil
			},
		},
		{
			Name:      "scp",
			Usage:     "copy files from/to EMR cluster",
			ArgsUsage: "NAME SOURCES... DEST",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "i",
					EnvVar: "EMR_SSH_IDENTITY_FILE",
				},
				cli.StringSliceFlag{
					Name:   "o",
					EnvVar: "ENV_SSH_OPTIONS",
					Value: &cli.StringSlice{
						"ServerAliveInterval=10",
						"StrictHostKeyChecking=no",
						"UserKnownHostsFile=/dev/null",
					},
					Usage: "SSH options in KEY=VAL format",
				},
				cli.BoolFlag{
					Name: "debug, d",
				},
			},
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 1, -1)

				name := c.Args().Get(0)
				args := c.Args()[1:]

				err := a.SCP(&AppSCPOptions{
					Name:         name,
					Args:         args,
					IdentityFile: c.String("i"),
					Options:      parseVariables(c.StringSlice("o")),
					Debug:        c.Bool("debug"),
				})
				if err != nil {
					return cli.NewExitError(err, 1)
				}

				return nil
			},
		},
		{
			Name:      "shell",
			Usage:     "set master uri to EMR_MASTER environment variable",
			ArgsUsage: "NAME",
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 1, -1)

				name := c.Args().Get(0)
				args := c.Args()[1:]

				err := a.Shell(name, args)
				if err != nil {
					return cli.NewExitError(err, 1)
				}

				return nil
			},
		},
		{
			Name:      "init",
			Usage:     "print initialization script for shell helper",
			ArgsUsage: "[COMMAND_NAME]",
			Action: func(c *cli.Context) error {
				validateArgsLength(c, 0, 1)

				var name string
				if len(c.Args()) == 1 {
					name = c.Args().Get(0)
				} else {
					name = "emrcmd"
				}
				output :=
					name + `() {
  local command
  command="$1"
  if [ "$#" -gt 0 ]; then
    shift
  fi

  case "$command" in
  shell)
    if [ "$#" -eq 1 ]; then
      eval "$(command emrcmd shell "$@")"
    else
      command emrcmd shell "$@"
    fi
    ;;
  *)
    command emrcmd "$command" "$@";;
  esac
}
`
				fmt.Fprintln(a.Stdout, output)

				return nil
			},
		},
	}

	return app
}

func validateArgsLength(c *cli.Context, min int, max int) {
	l := len(c.Args())

	var n string
	var ok bool

	if min < 0 && max < 0 {
		return
	} else if min < 0 {
		n = fmt.Sprintf("at most %d", max)
		ok = l <= max
	} else if max < 0 {
		n = fmt.Sprintf("at least %d", min)
		ok = l >= min
	} else if min == max {
		n = fmt.Sprintf("%d", min)
		ok = l == min
	} else {
		n = fmt.Sprintf("%d..%d", min, max)
		ok = min <= l && l <= max
	}

	if !ok {
		fmt.Fprintf(cli.ErrWriter, "Error: %s arguments expected, but got %d\n", n, l)
		cli.OsExiter(1)
	}
}

func parseVariables(args []string) map[string]string {
	m := map[string]string{}
	for _, o := range args {
		kv := strings.SplitN(o, "=", 2)
		if len(kv) == 2 {
			m[kv[0]] = kv[1]
		} else if kv[0] != "" {
			m[kv[0]] = ""
		}
	}
	return m
}
