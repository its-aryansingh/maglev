package restapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"maglev.onebusaway.org/internal/app"
	"maglev.onebusaway.org/internal/appconf"
	"maglev.onebusaway.org/internal/clock"
	"maglev.onebusaway.org/internal/gtfs"
	"maglev.onebusaway.org/internal/models"
)

// TestHandlerLockingDuringForceUpdate verifies that HTTP handlers correctly hold locks
// while a GTFS static data swap is in progress.
//
// This is an integration test that:
// 1. Starts the HTTP server with GTFS data loaded
// 2. Triggers ForceUpdate in a goroutine
// 3. Issues concurrent HTTP requests during the swap
// 4. Verifies requests complete safely (no panics, errors, or races)
//
// Fixes #249
func TestHandlerLockingDuringForceUpdate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on Windows: SQLite file I/O is too slow for CI timeout")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test-force-update.db")

	// Initialize with raba.zip (agency ID: 25)
	gtfsConfig := gtfs.Config{
		GtfsURL:      filepath.Join("../../testdata", "raba.zip"),
		GTFSDataPath: dbPath,
		Env:          appconf.Development,
	}

	manager, err := gtfs.InitGTFSManager(gtfsConfig)
	require.NoError(t, err)
	defer manager.Shutdown()

	// Verify initial state
	manager.RLock()
	initialAgencyID := manager.GetAgencies()[0].Id
	manager.RUnlock()
	assert.Equal(t, "25", initialAgencyID, "Should start with agency 25")

	// Create REST API
	application := &app.Application{
		Config: appconf.Config{
			Env:       appconf.Development,
			ApiKeys:   []string{"TEST"},
			RateLimit: 1000,
		},
		GtfsConfig:  gtfsConfig,
		GtfsManager: manager,
		Clock:       clock.RealClock{},
	}
	api := NewRestAPI(application)
	defer api.Shutdown()

	// Setup test server
	mux := http.NewServeMux()
	api.SetRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Track results
	var successfulRequests int64
	var failedRequests int64
	errChan := make(chan error, 100)

	// Start concurrent HTTP requests
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readerCount := 10
	var wg sync.WaitGroup
	wg.Add(readerCount)

	// Launch reader goroutines that make continuous HTTP requests
	for i := 0; i < readerCount; i++ {
		go func(id int) {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Make HTTP request to agencies endpoint
					resp, err := client.Get(server.URL + "/api/where/agencies-with-coverage.json?key=TEST")
					if err != nil {
						if ctx.Err() == nil {
							errChan <- err
							atomic.AddInt64(&failedRequests, 1)
						}
						continue
					}

					// Decode response
					var response models.ResponseModel
					if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
						resp.Body.Close()
						if ctx.Err() == nil {
							errChan <- err
							atomic.AddInt64(&failedRequests, 1)
						}
						continue
					}
					resp.Body.Close()

					// Verify response is valid
					if response.Code != 200 {
						if ctx.Err() == nil {
							atomic.AddInt64(&failedRequests, 1)
						}
						continue
					}

					atomic.AddInt64(&successfulRequests, 1)
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let readers establish steady state
	time.Sleep(100 * time.Millisecond)

	// Trigger ForceUpdate to swap GTFS data (from raba.zip to gtfs.zip)
	newSource := filepath.Join("../../testdata", "gtfs.zip")
	manager.SetGtfsSource(newSource) // Use setter if available, otherwise direct access

	updateErr := manager.ForceUpdate(context.Background())

	// Let readers continue during and after the update
	time.Sleep(200 * time.Millisecond)

	// Stop readers
	cancel()
	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for e := range errChan {
		errors = append(errors, e)
	}

	// Assertions
	assert.NoError(t, updateErr, "ForceUpdate should succeed")
	assert.Empty(t, errors, "No HTTP request errors should occur during ForceUpdate")
	assert.Zero(t, failedRequests, "No requests should fail during ForceUpdate")
	assert.Greater(t, successfulRequests, int64(0), "Should have completed some requests")

	t.Logf("Completed %d successful requests, %d failed requests", successfulRequests, failedRequests)

	// Verify final state - should now have agency 40 from gtfs.zip
	manager.RLock()
	finalAgencyID := manager.GetAgencies()[0].Id
	manager.RUnlock()
	assert.Equal(t, "40", finalAgencyID, "Should end with agency 40 after ForceUpdate")
}

// TestHandlerLockingDuringForceUpdate_MultipleEndpoints tests multiple different
// endpoints concurrently during ForceUpdate.
func TestHandlerLockingDuringForceUpdate_MultipleEndpoints(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on Windows: SQLite file I/O is too slow for CI timeout")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test-force-update-multi.db")

	gtfsConfig := gtfs.Config{
		GtfsURL:      filepath.Join("../../testdata", "raba.zip"),
		GTFSDataPath: dbPath,
		Env:          appconf.Development,
	}

	manager, err := gtfs.InitGTFSManager(gtfsConfig)
	require.NoError(t, err)
	defer manager.Shutdown()

	application := &app.Application{
		Config: appconf.Config{
			Env:       appconf.Development,
			ApiKeys:   []string{"TEST"},
			RateLimit: 1000,
		},
		GtfsConfig:  gtfsConfig,
		GtfsManager: manager,
		Clock:       clock.RealClock{},
	}
	api := NewRestAPI(application)
	defer api.Shutdown()

	mux := http.NewServeMux()
	api.SetRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	endpoints := []string{
		"/api/where/agencies-with-coverage.json?key=TEST",
		"/api/where/current-time.json?key=TEST",
		"/api/where/routes-for-agency/25.json?key=TEST",
	}

	var successfulRequests int64
	var failedRequests int64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(len(endpoints))

	// Launch one reader per endpoint
	for _, endpoint := range endpoints {
		go func(ep string) {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}

			for {
				select {
				case <-ctx.Done():
					return
				default:
					resp, err := client.Get(server.URL + ep)
					if err != nil {
						if ctx.Err() == nil {
							atomic.AddInt64(&failedRequests, 1)
						}
						continue
					}

					var response models.ResponseModel
					if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
						resp.Body.Close()
						if ctx.Err() == nil {
							atomic.AddInt64(&failedRequests, 1)
						}
						continue
					}
					resp.Body.Close()

					if response.Code == 200 {
						atomic.AddInt64(&successfulRequests, 1)
					} else {
						atomic.AddInt64(&failedRequests, 1)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(endpoint)
	}

	time.Sleep(50 * time.Millisecond)

	// Trigger ForceUpdate
	newSource := filepath.Join("../../testdata", "gtfs.zip")
	manager.SetGtfsSource(newSource)
	updateErr := manager.ForceUpdate(context.Background())

	time.Sleep(150 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.NoError(t, updateErr, "ForceUpdate should succeed")
	assert.Zero(t, failedRequests, "No requests should fail during ForceUpdate")
	assert.Greater(t, successfulRequests, int64(0), "Should have completed some requests")

	t.Logf("Multi-endpoint: %d successful, %d failed", successfulRequests, failedRequests)
}
