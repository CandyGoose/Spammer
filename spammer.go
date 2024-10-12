package main

import (
	"fmt"
	"sort"
	"sync"
)

// RunPipeline запускает команды последовательно, передавая выходные данные каждой команды в следующую
func RunPipeline(cmds ...cmd) {
	var in chan interface{}
	done := make(chan struct{})
	numGoroutines := len(cmds)

	for _, cmdFunc := range cmds {
		out := make(chan interface{})
		go func(cmdFunc cmd, in, out chan interface{}, done chan<- struct{}) {
			cmdFunc(in, out)
			close(out)
			done <- struct{}{}
		}(cmdFunc, in, out, done)
		in = out
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// SelectUsers получает уникальных пользователей и отправляет их в выходной канал
func SelectUsers(in, out chan interface{}) {
	done := make(chan struct{})
	uniqueUsers := make(map[uint64]struct{})
	var mu sync.Mutex
	numGoroutines := 0

	for data := range in {
		email := data.(string)
		numGoroutines++
		go func(email string, done chan<- struct{}) {
			user := GetUser(email)

			mu.Lock()
			if _, exists := uniqueUsers[user.ID]; !exists {
				uniqueUsers[user.ID] = struct{}{}
				out <- user
			}
			mu.Unlock()

			done <- struct{}{}
		}(email, done)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// SelectMessages собирает пользователей в батчи и получает их сообщения
func SelectMessages(in, out chan interface{}) {
	users := make([]User, 0, GetMessagesMaxUsersBatch)
	done := make(chan struct{})
	batchCount := 0

	for userData := range in {
		user := userData.(User)
		users = append(users, user)

		if len(users) == GetMessagesMaxUsersBatch {
			batchCount++
			go selectWorker(users, out, done)
			users = make([]User, 0, GetMessagesMaxUsersBatch)
		}
	}

	if len(users) > 0 {
		batchCount++
		go selectWorker(users, out, done)
	}

	for i := 0; i < batchCount; i++ {
		<-done
	}
}

func selectWorker(u []User, out chan<- interface{}, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	res, err := GetMessages(u...)
	if err != nil {
		return
	}
	for _, msg := range res {
		out <- msg
	}
}

// CheckSpam проверяет сообщения на спам
func CheckSpam(in, out chan interface{}) {
	done := make(chan struct{})
	numWorkers := HasSpamMaxAsyncRequests

	for i := 0; i < numWorkers; i++ {
		go func() {
			for id := range in {
				id := id.(MsgID)

				result, err := HasSpam(id)
				if err != nil {
					continue
				}
				out <- MsgData{id, result}
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}
}

// CombineResults сортирует результаты и выводит их
func CombineResults(in, out chan interface{}) {
	const preallocateSize = 100
	results := make([]MsgData, 0, preallocateSize)
	for data := range in {
		msgData := data.(MsgData)
		results = append(results, msgData)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam && !results[j].HasSpam
		}
		return results[i].ID < results[j].ID
	})
	for _, msgData := range results {
		out <- fmt.Sprintf("%v %v", msgData.HasSpam, msgData.ID)
	}
}
