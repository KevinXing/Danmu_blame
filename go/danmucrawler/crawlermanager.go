package danmucrawler

import (
	"context"
	"log"

	"github.com/samsarahq/go/oops"
)

const (
	defualtManagerCapacity = 100
)

type CrawlerId string

type Crawler interface {
	Run(ctx context.Context) error
	Id() CrawlerId
}

type crawlerManager struct {
	crawlerMap map[CrawlerId]Crawler

	// The max number of crawlers the manager can hold.
	capacity int

	// This channel holds non-started crawlers, include the retry crawlers
	waitForRunChan chan Crawler
}

func NewCrawlerManager(ctx context.Context) *crawlerManager {
	return &crawlerManager{
		crawlerMap:     make(map[CrawlerId]Crawler, defualtManagerCapacity),
		capacity:       defualtManagerCapacity,
		waitForRunChan: make(chan Crawler, defualtManagerCapacity),
	}
}

func (cm *crawlerManager) Register(ctx context.Context, c Crawler) error {
	if len(cm.crawlerMap) == cm.capacity {
		return oops.Errorf("crawler manager reach capacity: %d crawlers", cm.capacity)
	}
	id := c.Id()
	if _, ok := cm.crawlerMap[id]; ok {
		return oops.Errorf("crawler %s already registerd", id)
	}
	cm.crawlerMap[id] = c
	cm.waitForRunChan <- c
	return nil
}

func (cm *crawlerManager) Run(ctx context.Context) error {
	for c := range cm.waitForRunChan {
		go func(c Crawler) {
			ctx := ctx
			err := c.Run(ctx)
			log.Printf("crawler %s error: %s, retry\n", c.Id(), err.Error())
			cm.waitForRunChan <- c
		}(c)
	}
	select {
	case <-ctx.Done():
		close(cm.waitForRunChan)
		return oops.Wrapf(ctx.Err(), "ctx cancelled")
	}
}
