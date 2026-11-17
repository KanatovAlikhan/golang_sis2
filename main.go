package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	store := NewTaskStore(StatisticsProcessor{})
	pool, err := NewWorkerPool(store, DefaultWorkerCount, DefaultQueueCapacity)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		return
	}
	defer func() {
		fmt.Println("\nFinishing accepted tasks...")
		pool.Shutdown()
		for _, task := range store.ListTasks() {
			printTask(task)
		}
		fmt.Println("Goodbye!")
	}()
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("SIS Checkpoint 2 - Concurrent Statistics Calculator")
	fmt.Printf("Workers: %d | Queue capacity: %d\n", DefaultWorkerCount, DefaultQueueCapacity)
	for {
		fmt.Println("\n1. Create a task")
		fmt.Println("2. Submit a task for processing")
		fmt.Println("3. Get a task by ID")
		fmt.Println("4. List all tasks")
		fmt.Println("0. Exit")
		fmt.Print("Choice: ")
		if !scanner.Scan() {
			return
		}

		choice := strings.TrimSpace(scanner.Text())
		switch choice {
		case "1":
			fmt.Print("Enter numbers separated by spaces: ")
			if !scanner.Scan() {
				return
			}
			numbers, err := parseNumbers(scanner.Text())
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			task, err := store.CreateTask(numbers)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			fmt.Printf("Created task #%d. Status: %s\n", task.ID, task.Status)
		case "2", "3":
			fmt.Print("Task ID: ")
			if !scanner.Scan() {
				return
			}
			id, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
			if err != nil {
				fmt.Println("Error: task ID must be an integer")
				continue
			}
			if choice == "2" {
				if err := pool.Submit(id); err != nil {
					fmt.Println("Error:", err)
				} else {
					fmt.Printf("Task #%d accepted for asynchronous processing. Use Get or List to see its result.\n", id)
				}
				continue
			}
			task, err := store.GetTask(id)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			printTask(task)
		case "4":
			tasks := store.ListTasks()
			if len(tasks) == 0 {
				fmt.Println("No tasks yet.")
			}
			for _, task := range tasks {
				printTask(task)
			}
		case "0":
			return
		default:
			fmt.Println("Unknown choice.")
		}
	}
}

func parseNumbers(input string) ([]float64, error) {
	fields := strings.Fields(input)
	numbers := make([]float64, 0, len(fields))
	for _, field := range fields {
		number, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a valid number: %w", field, err)
		}
		numbers = append(numbers, number)
	}
	return numbers, nil
}

func printTask(task Task) {
	fmt.Printf("Task #%d | Numbers: %v | Status: %s\n", task.ID, task.Numbers, task.Status)
	if task.Error != "" {
		fmt.Println("Processing error:", task.Error)
	}
	if task.Result != nil {
		result := task.Result
		fmt.Printf("Result: count=%d min=%.2f max=%.2f sum=%.2f average=%.2f\n",
			result.Count, result.Min, result.Max, result.Sum, result.Average)
	}
}
