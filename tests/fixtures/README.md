# Test fixtures

## `isamples_export_fixture.parquet` (140 rows, ~31 kB)

Real records from the iSamples complete export of 2025-04-21, Zenodo
[doi:10.5281/zenodo.15278211](https://doi.org/10.5281/zenodo.15278211), file
`isamples_export_2025_04_21_16_23_46_geo.parquet` (md5 `081e2dd50da9a807e182a994790cfedb`).

**Selection (deterministic):** per `source_collection`, the 40 lowest `sample_identifier`s among
rows with a sampling site and at least two keywords, unioned with the 20 lowest among rows with a
sampling site; ordered by `sample_identifier`. Result: GEOME 40, OPENCONTEXT 40, SESAR 20,
SMITHSONIAN 40; 120 rows with multi-valued keywords, 8 site keys shared by more than one row,
40 rows with `related_resource`, 80 with `curation`. Chosen so every ordering path in
`sql_converter` (site de-duplication, multi-valued edges, curation, relations) is exercised.

**Attribution and license:** the export aggregates records from SESAR, Open Context, GEOME and the
Smithsonian NMNH; each source retains its own terms (see https://isamples.org/data.html), and the
Zenodo export record is published under CC BY-NC-SA 4.0. This fixture is redistributed here for
the sole purpose of testing the converter; cite both the source collections and the iSamples
export when reusing. Regenerate with the query in `tests/test_determinism.py`'s module docstring
if the export changes.
