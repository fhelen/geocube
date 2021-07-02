package grid_test

import (
	"context"

	. "github.com/airbusgeo/geocube/internal/utils/grid"
	"github.com/airbusgeo/godal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/twpayne/go-geom"
)

var _ = Describe("SingleCellGrid", func() {
	var (
		ctx            = context.Background()
		singleCellGrid Grid
		geomAoi        *geom.MultiPolygon
		crs            *godal.SpatialRef
	)

	BeforeEach(func() {
		crs, _ = godal.NewSpatialRefFromEPSG(32631)
		singleCellGrid, _ = NewGrid([]string{}, map[string]string{
			"grid":       "singlecell",
			"crs":        "+proj=utm +zone=31",
			"resolution": "10"})

		geomAoi = toMultiPolygon([][2]float64{{5.8, 45.1}, {5.8, 44.5}, {6.6, 44.5}, {6.6, 45.1}, {5.8, 45.1}})
	})

	var (
		itShouldNotReturnedAnError = func(err error) {
			It("it should not return an error", func() {
				Expect(err).To(BeNil())
			})
		}
	)

	Describe("Cover", func() {

		var (
			returnedCover    []string
			returnedCoverErr error
		)

		JustBeforeEach(func() {
			covers, err := singleCellGrid.Covers(ctx, geomAoi)
			returnedCover = nil
			if err != nil {
				returnedCoverErr = err
			} else {
				for c := range covers {
					returnedCover = append(returnedCover, c)
				}
			}
		})

		var (
			itShouldReturnedRightCover = func() {
				It("it should return right cover response", func() {
					Expect(returnedCover).To(Equal([]string{"720298.429720/5000366.394350/6590/6914"}))
				})
			}
		)

		Context("default", func() {
			itShouldNotReturnedAnError(returnedCoverErr)
			itShouldReturnedRightCover()
		})
	})

	Describe("Cell", func() {
		var (
			returnedCell    *Cell
			returnedCellErr error
		)

		JustBeforeEach(func() {
			returnedCell, returnedCellErr = singleCellGrid.Cell("720298.429720/5000366.394350/6590/6914")
		})

		var (
			itShouldReturnedRightCell = func() {
				It("it should return right cover response", func() {
					Expect(returnedCell.SizeX).To(Equal(6590))
					Expect(returnedCell.SizeY).To(Equal(6914))
					Expect(returnedCell.CRS.IsSame(crs)).To(BeTrue())
					Expect(returnedCell.URI).To(Equal("720298.429720/5000366.394350/6590/6914"))

					json, err := CellsToJSON(singleCellGrid, []string{"720298.429720/5000366.394350/6590/6914"})
					Expect(err).To(BeNil())
					Expect(json).To(MatchJSON(`{"type":"MultiPolygon","coordinates":[[[[5.801096927991,45.12241193432],[5.771142340276,44.500735020838],[6.598660882864,44.47763583486],[6.637518180705,45.098809626962],[5.801096927991,45.12241193432]]]]}`))
				})
			}
		)

		Context("default", func() {
			itShouldNotReturnedAnError(returnedCellErr)
			itShouldReturnedRightCell()
		})
	})

})
