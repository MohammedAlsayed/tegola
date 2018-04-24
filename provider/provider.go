package provider

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-spatial/geom"
	"github.com/go-spatial/tegola/internal/log"
)

type Tile interface {
	// ZXY returns the z, x and y values of the tile
	ZXY() (uint, uint, uint)
	// Extent returns the extent of the tile excluding any buffer
	Extent() (extent *geom.Extent, srid uint64)
	// BufferedExtent returns the extent of the tile including any buffer
	BufferedExtent() (extent *geom.Extent, srid uint64)
}

type Tiler interface {
	// TileFeature will stream decoded features to the callback function fn
	// if fn returns ErrCanceled, the TileFeatures method should stop processing
	TileFeatures(ctx context.Context, layer string, t Tile, fn func(f *Feature) error) error
	// Layers returns information about the various layers the provider supports
	Layers() ([]LayerInfo, error)
}

type TimeExtent interface {
	StartTime() *time.Time
	EndTime() *time.Time
}

type IndexExtent interface {
	StartIndex() uint
	EndIndex() uint
}

type Bounder interface {
	TimeExtent() *TimeExtent
	GeomExtent() *geom.Extent
	IndexExtent() *IndexExtent
}

type FeatureConsumer func(f *Feature) error

// Returns features in a consistent order.

// Limits features to those contained within the time, geometrical, and index bounds.
// Features w/o time data will be considered to be within all time bounds.
// Features w/o goemetry data will be considered to be within all geometrical extents.
// A nil value returned from any of the Bounder methods indicates no filtering for that dimension.
type Filterer interface {
	StreamFeatures(
		ctx context.Context, layer string, // Nothing new here
		bounds Bounder, // Combine Time, Space, and index bounding
		properties map[string]string, // Properties to filter on.
		// If the feature has any of the properties named,
		//	the property values must match (fuzzily, i.e. conversion
		//	from string to native type then match) for the
		//	feature to be returned.  nil indicates no property
		//	filtering.
		fn FeatureConsumer, // The "Feature" type this takes as an argument has been modified
	) error
	Layers() ([]LayerInfo, error)
}

type LayerInfo interface {
	Name() string
	GeomType() geom.Geometry
	SRID() uint64
	// To support caching, a value that only changes if data in the layer has changed.
	//	nil indicates the provider doesn't support or is unable to provide this
	ModificationTag() *string
}

// InitFunc initilize a provider given a config map. The init function should validate the config map, and report any errors. This is called by the For function.
type InitFunc func(map[string]interface{}) (Tiler, error)

// CleanupFunc is called to when the system is shuting down, this allows the provider to cleanup.
type CleanupFunc func()

type pfns struct {
	init    InitFunc
	cleanup CleanupFunc
}

var providers map[string]pfns

// Register the provider with the system. This call is generally made in the init functions of the provider.
// 	the clean up function will be called during shutdown of the provider to allow the provider to do any cleanup.
func Register(name string, init InitFunc, cleanup CleanupFunc) error {
	if providers == nil {
		providers = make(map[string]pfns)
	}

	if _, ok := providers[name]; ok {
		return fmt.Errorf("Provider %v already exists", name)
	}

	providers[name] = pfns{
		init:    init,
		cleanup: cleanup,
	}

	return nil
}

// Drivers returns a list of registered drivers.
func Drivers() (l []string) {
	if providers == nil {
		return l
	}

	for k := range providers {
		l = append(l, k)
	}

	return l
}

// For function returns a configured provider of the given type, provided the correct config map.
func For(name string, config map[string]interface{}) (Tiler, error) {
	if providers == nil {
		return nil, fmt.Errorf("No providers registered.")
	}

	p, ok := providers[name]
	if !ok {
		return nil, fmt.Errorf("No providers registered by the name: %v, known providers(%v)", name, strings.Join(Drivers(), ","))
	}

	return p.init(config)
}

func Cleanup() {
	log.Info("cleaning up providers")
	for _, p := range providers {
		if p.cleanup != nil {
			p.cleanup()
		}
	}
}
