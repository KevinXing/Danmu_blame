package main

import (
	"context"
	"log"

	"github.com/KevinXing/Danmu_blame/go/src/danmucrawler"
)

func main() {
	ctx := context.Background()

	crawlerManager, err := danmucrawler.NewCrawlerManager(ctx)
	if err != nil {
		panic(err)
	}
	testCrawler1, err := danmucrawler.NewDouyuDanmuCrawler(ctx, "60937")
	if err != nil {
		panic(err)
	}
	testCrawler2, err := danmucrawler.NewDouyuDanmuCrawler(ctx, "5650069")
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
