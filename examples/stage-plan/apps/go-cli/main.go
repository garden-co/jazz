// Command go-cli is StagePlan's stage-prep board in a terminal: a Go
// backend that acts as one signed-in user. It syncs with the Jazz server
// that JAZZ_SERVER_URL, JAZZ_APP_ID, JAZZ_BACKEND_SECRET and
// JAZZ_ADMIN_SECRET name.
//
//	go-cli [-user alice] [-data .jazz] show            # your show (created if needed)
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

	"github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp"
)

func main() {
	user := flag.String("user", "go-cli-user", "the user the CLI acts as")
	data := flag.String("data", ".jazz", "where the rows are kept (empty: in memory)")
	showName := flag.String("show", "Go Night", "the name of a show the user creates")
	flag.Parse()
	if err := run(context.Background(), *user, *data, *showName, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "go-cli:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, user, data, showName string, args []string) error {
	if len(args) == 0 {
		args = []string{"tasks"}
	}
	srv := server{
		appID:         os.Getenv("JAZZ_APP_ID"),
		url:           os.Getenv("JAZZ_SERVER_URL"),
		backendSecret: os.Getenv("JAZZ_BACKEND_SECRET"),
		adminSecret:   os.Getenv("JAZZ_ADMIN_SECRET"),
	}
	db, err := openBackend(ctx, srv, user, data)
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
