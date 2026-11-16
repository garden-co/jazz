// Command go-cli is StagePlan's stage-prep board in a terminal, as one of
// its users: it signs in through the app's Jazz endpoint (JAZZ_SERVER_URL,
// its main backend's /jazz) with JAZZ_TOKEN, a token from the app's auth
// provider. JAZZ_APP_ID defaults to the id a TypeScript client derives
// from the URL.
//
//	go-cli [-data .jazz] show                          # your show (created if needed)
//	go-cli tasks                                       # its tasks, by rank
//	go-cli add "Line check"                            # add a task to To do
//	go-cli move <task-id> doing                        # todo, doing, done or blocked
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

func main() {
	data := flag.String("data", ".jazz", "where the rows are kept (empty: in memory)")
	showName := flag.String("show", "Go Night", "the name of a show the user creates")
	flag.Parse()
	if err := run(context.Background(), os.Getenv("JAZZ_TOKEN"), *data, *showName, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "go-cli:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, token, data, showName string, args []string) error {
	if len(args) == 0 {
		args = []string{"tasks"}
	}
	srv := server{appID: os.Getenv("JAZZ_APP_ID"), url: os.Getenv("JAZZ_SERVER_URL")}
	if srv.appID == "" {
		id, err := jazz.EndpointAppID(srv.url)
		if err != nil {
			return err
		}
		srv.appID = id
	}
	db, err := openUser(ctx, srv, token, data)
	if err != nil {
		return err
	}
	defer db.Close()
	show, err := ensureShow(ctx, db, showName)
	if err != nil {
		return err
	}
	switch args[0] {
	case "show":
		fmt.Printf("%s at %s, %s (doors %s)\n%s\n", show.Name, show.Venue, show.Date, show.Doors, show.ID)
		return nil
	case "tasks":
		tasks, err := listTasks(ctx, db, show.ID)
		if err != nil {
			return err
		}
		for _, status := range statuses {
			fmt.Printf("%s\n", strings.ToUpper(string(status)))
			for _, t := range tasks {
				if t.Status == status {
					fmt.Printf("  %s  %s\n", t.ID, t.Title)
				}
			}
		}
		return nil
	case "add":
		if len(args) != 2 {
			return fmt.Errorf("usage: add <title>")
		}
		task, err := addTask(ctx, db, show.ID, args[1])
		if err != nil {
			return err
		}
		fmt.Println(task.ID)
		return nil
	case "move":
		if len(args) != 3 {
			return fmt.Errorf("usage: move <task-id> todo|doing|done|blocked")
		}
		status := jazzapp.TaskStatus(args[2])
		for _, s := range statuses {
			if s == status {
				return moveTask(ctx, db, args[1], status)
			}
		}
		return fmt.Errorf("%q is not a stage: todo, doing, done or blocked", args[2])
	default:
		return fmt.Errorf("unknown command %q: show, tasks, add or move", args[0])
	}
}
