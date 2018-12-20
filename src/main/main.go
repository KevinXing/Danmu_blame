package main

import (
	"context"
	"danmucrawler"
	"log"
)

func main() {
	ctx := context.Background()

	crawlerManager := danmucrawler.NewCrawlerManager(ctx)
	testCrawler1, err := danmucrawler.NewDouyuDanmuCrawler("74960")
	if err != nil {
		panic(err)
	}
	testCrawler2, err := danmucrawler.NewDouyuDanmuCrawler("5650069")
	if err != nil {
		panic(err)
	}

	if err := crawlerManager.Register(ctx, testCrawler1); err != nil {
		log.Println("fail to register testCrawler1")
	}

	if err := crawlerManager.Register(ctx, testCrawler2); err != nil {
		log.Println("fail to register testCrawler2")
	}

	crawlerManager.Run(ctx)
}
