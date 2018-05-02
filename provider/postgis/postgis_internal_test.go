package postgis

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/go-spatial/geom"
	"github.com/go-spatial/tegola/internal/ttools"
	"github.com/jackc/pgx"
)

// TESTENV is the environment variable that must be set to "yes" to run postgis tests.
const TESTENV = "RUN_POSTGIS_TESTS"

func GetTestPort(t *testing.T) int64 {
	ttools.ShouldSkip(t, TESTENV)
	port, err := strconv.ParseInt(os.Getenv("PGPORT"), 10, 64)
	if err != nil {
		t.Skipf("err parsing PGPORT: %v", err)
	}
	return port
}

func TestLayerGeomType(t *testing.T) {
	port := GetTestPort(t)

	testcases := []struct {
		config    map[string]interface{}
		layerName string
		geom      geom.Geometry
	}{
		{
			config: map[string]interface{}{
				ConfigKeyHost:     os.Getenv("PGHOST"),
				ConfigKeyPort:     port,
				ConfigKeyDB:       os.Getenv("PGDATABASE"),
				ConfigKeyUser:     os.Getenv("PGUSER"),
				ConfigKeyPassword: os.Getenv("PGPASSWORD"),
				ConfigKeyLayers: []map[string]interface{}{
					{
						ConfigKeyLayerName: "land",
						ConfigKeySQL:       "SELECT gid, ST_AsBinary(geom) FROM ne_10m_land_scale_rank WHERE geom && !BBOX!",
					},
				},
			},
			layerName: "land",
			geom:      geom.MultiPolygon{},
		},
		// zoom token replacement
		{
			config: map[string]interface{}{
				ConfigKeyHost:     os.Getenv("PGHOST"),
				ConfigKeyPort:     port,
				ConfigKeyDB:       os.Getenv("PGDATABASE"),
				ConfigKeyUser:     os.Getenv("PGUSER"),
				ConfigKeyPassword: os.Getenv("PGPASSWORD"),
				ConfigKeyLayers: []map[string]interface{}{
					{
						ConfigKeyLayerName: "land",
						ConfigKeySQL:       "SELECT gid, ST_AsBinary(geom) FROM ne_10m_land_scale_rank WHERE gid = !ZOOM! AND geom && !BBOX!",
					},
				},
			},
			layerName: "land",
			geom:      geom.MultiPolygon{},
		},
	}

	for i, tc := range testcases {
		provider, err := NewTileProvider(tc.config)
		if err != nil {
			t.Errorf("[%v] NewProvider error, expected nil got %v", i, err)
			continue
		}

		p := provider.(Provider)
		layer := p.layers[tc.layerName]
		if err := p.layerGeomType(&layer); err != nil {
			t.Errorf("[%v] layerGeomType error, expected nil got %v", i, err)
			continue
		}

		if !reflect.DeepEqual(tc.geom, layer.geomType) {
			t.Errorf("[%v] geom type, expected %v got %v", i, tc.geom, layer.geomType)
			continue
		}
	}
}

func TestDecipherFields(t *testing.T) {
	ttools.ShouldSkip(t, TESTENV)
	cc := pgx.ConnConfig{
		Host:     os.Getenv("PGHOST"),
		Port:     5432,
		Database: os.Getenv("PGDATABASE"),
		User:     os.Getenv("PGUSER"),
		Password: os.Getenv("PGPASSWORD"),
	}

	type TestCase struct {
		id           int32
		expectedTags map[string]string
	}

	testCases := []TestCase{
		{
			id:           1,
			expectedTags: map[string]string{"height": "9", "id": "1"},
		},
		{
			id:           2,
			expectedTags: map[string]string{"hello": "there", "good": "day"},
		},
	}

	conn, err := pgx.Connect(cc)
	if err != nil {
		t.Errorf("Unable to connect to database: %v", err)
	}
	defer conn.Close()

	for _, tc := range testCases {
		sql := fmt.Sprintf("SELECT id, tags FROM hstore_test WHERE id = %v;", tc.id)
		rows, err := conn.Query(sql)
		if err != nil {
			t.Errorf("Error performing query: %v", err)
		}
		defer rows.Close()

		i := 0
		for rows.Next() {
			geoFieldname := "geom"
			idFieldname := "id"
			descriptions := rows.FieldDescriptions()
			vals, err := rows.Values()
			if err != nil {
				t.Errorf("[%v] Problem collecting row values", i)
			}

			_, _, tags, err := decipherFields(context.TODO(), geoFieldname, idFieldname, descriptions, vals)
			for k, v := range tags {
				if tc.expectedTags[k] != v {
					t.Errorf("[%v] Missing or bad value for tag %v: %v != %v", i, k, v, tc.expectedTags[k])
				}
			}
			i++
		}
	}
}
