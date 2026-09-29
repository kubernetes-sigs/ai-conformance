package conformance

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of Kubernetes AI Conformance tests:\n\n")
		fmt.Fprintf(os.Stderr, "These tests require specific flags depending on the capability being tested.\n\n")

		var globalFlags, autoscalerFlags, gangFlags, sandboxFlags, goTestFlags []*flag.Flag

		flag.VisitAll(func(f *flag.Flag) {
			if strings.HasPrefix(f.Name, "test.") {
				goTestFlags = append(goTestFlags, f)
			} else if strings.HasPrefix(f.Name, "autoscaler-") {
				autoscalerFlags = append(autoscalerFlags, f)
			} else if strings.HasPrefix(f.Name, "gang-") {
				gangFlags = append(gangFlags, f)
			} else if strings.HasPrefix(f.Name, "sandbox-") {
				sandboxFlags = append(sandboxFlags, f)
			} else {
				globalFlags = append(globalFlags, f)
			}
		})

		printFlags := func(title string, flags []*flag.Flag) {
			if len(flags) == 0 {
				return
			}
			fmt.Fprintf(os.Stderr, "%s:\n", title)
			for _, f := range flags {
				// Use exactly the same format as Go's default flag printing
				_, _ = fmt.Fprintf(os.Stderr, "  -%s", f.Name)
				name, usage := flag.UnquoteUsage(f)
				if len(name) > 0 {
					_, _ = fmt.Fprintf(os.Stderr, " %s", name)
				}
				_, _ = fmt.Fprintf(os.Stderr, "\n    \t%s", strings.ReplaceAll(usage, "\n", "\n    \t"))
				if f.DefValue != "" {
					_, _ = fmt.Fprintf(os.Stderr, " (default %q)", f.DefValue)
				}
				_, _ = fmt.Fprintf(os.Stderr, "\n")
			}
			fmt.Fprintf(os.Stderr, "\n")
		}

		printFlags("Global Suite Flags (Apply to all tests)", globalFlags)
		printFlags("Accelerator Cluster Autoscaling Flags (TestAcceleratorClusterAutoscaling)", autoscalerFlags)
		printFlags("Gang Scheduling Flags (TestGangScheduling)", gangFlags)
		printFlags("Workload Sandboxing Flags (TestWorkloadSandboxing)", sandboxFlags)

		fmt.Fprintf(os.Stderr, "Standard Go Test Flags (e.g. -v, -run, -timeout, -short):\n")
		fmt.Fprintf(os.Stderr, "  Run 'go help testflag' for detailed documentation of standard flags.\n")
	}

	os.Exit(m.Run())
}
