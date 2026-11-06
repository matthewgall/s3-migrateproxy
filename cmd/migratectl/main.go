package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandSpec struct {
	method string
	path   string
}

var commands = map[string]commandSpec{
	"status":    {method: "GET", path: "/api/v1/status"},
	"progress":  {method: "GET", path: "/api/v1/migration/progress"},
	"start":     {method: "POST", path: "/api/v1/migration/start"},
	"resume":    {method: "POST", path: "/api/v1/migration/start"},
	"pause":     {method: "POST", path: "/api/v1/migration/pause"},
	"reconcile": {method: "POST", path: "/api/v1/migration/reconcile"},
	"finalise":  {method: "POST", path: "/api/v1/migration/finalise"},
	"finalize":  {method: "POST", path: "/api/v1/migration/finalise"},
	"errors":    {method: "GET", path: "/api/v1/migration/errors"},
	"report":    {method: "GET", path: "/api/v1/migration/report"},
	"health":    {method: "GET", path: "/api/v1/providers/health"},
}

var rootCmd = &cobra.Command{
	Use:   "migratectl",
	Short: "Control the s3-migrateproxy migration",
}

func main() {
	rootCmd.PersistentFlags().StringP("admin", "a", "http://localhost:8081", "admin API base URL")
	rootCmd.PersistentFlags().StringP("key", "k", os.Getenv("S3MIGRATE_ADMIN_API_KEY"), "admin API key")
	_ = viper.BindPFlag("admin_addr", rootCmd.PersistentFlags().Lookup("admin"))
	_ = viper.BindPFlag("admin_api_key", rootCmd.PersistentFlags().Lookup("key"))

	addRetryCommand()
	addObjectCommand()

	for name, spec := range commands {
		spec := spec
		rootCmd.AddCommand(&cobra.Command{
			Use:   name,
			Short: fmt.Sprintf("call %s %s", spec.method, spec.path),
			RunE: func(_ *cobra.Command, _ []string) error {
				return call(spec.method, spec.path)
			},
		})
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func addRetryCommand() {
	rootCmd.AddCommand(&cobra.Command{
		Use:   "retry <object-key>",
		Short: "Retry a failed object task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return call("POST", "/api/v1/migration/retry/"+args[0])
		},
	})
}

func addObjectCommand() {
	rootCmd.AddCommand(&cobra.Command{
		Use:   "object <object-key>",
		Short: "Get migration state for an object",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return call("GET", "/api/v1/migration/objects/"+args[0])
		},
	})
}

func call(method, path string) error {
	admin := viper.GetString("admin_addr")
	key := viper.GetString("admin_api_key")
	if key == "" {
		key = os.Getenv("S3MIGRATE_ADMIN_API_KEY_HASH")
	}

	req, err := http.NewRequest(method, strings.TrimSuffix(admin, "/")+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		os.Exit(1)
	}
	return nil
}
