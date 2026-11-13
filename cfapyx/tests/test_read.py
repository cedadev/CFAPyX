# All routines for testing CFA general methods.
import os
import tempfile
from unittest.mock import patch

import cftime
import netCDF4
import numpy as np
import pytest
import xarray as xr

from cfapyx import CFANetCDF
from cfapyx.filehandlers import NumpyDatasetHandler
from cfapyx.utils import set_verbose

set_verbose(2)

TESTDIR = "cfapyx/tests/test_space"


def _build_cfa_with_time_bnds(
    tmpdir: str,
    calendar: str = "standard",
    time_units: str = "days since 2000-01-01",
    bnds_units: str | None = None,
    start_offsets: list[float] | None = None,
) -> tuple[str, list[str]]:
    """Create 3 fragment NetCDF files with time, time_bnds, tas and aggregate."""
    files = []
    for i in range(3):
        fn = os.path.join(tmpdir, f"frag{i}.nc")
        base = start_offsets[i] if start_offsets is not None else float(i * 2)
        with netCDF4.Dataset(fn, "w") as nc:
            nc.createDimension("time", 2)
            nc.createDimension("bnds", 2)
            nc.createDimension("lat", 4)

            t = nc.createVariable("time", "f8", ("time",))
            t.units = time_units
            t.calendar = calendar
            t.bounds = "time_bnds"
            t[:] = [base + 0.5, base + 1.5]

            tb = nc.createVariable("time_bnds", "f8", ("time", "bnds"))
            if bnds_units is not None:
                tb.units = bnds_units
                tb.calendar = calendar
            scale = 24.0 if (bnds_units and bnds_units.startswith("hours")) else 1.0
            tb[:] = [
                [(base + 0.0) * scale, (base + 1.0) * scale],
                [(base + 1.0) * scale, (base + 2.0) * scale],
            ]

            lat = nc.createVariable("lat", "f8", ("lat",))
            lat[:] = [0.0, 1.0, 2.0, 3.0]

            tas = nc.createVariable("tas", "f4", ("time", "lat"))
            tas[:] = np.full((2, 4), float(i), dtype="f4")
        files.append(fn)

    cfa = CFANetCDF(files)
    cfa.create()
    nca_fn = os.path.join(tmpdir, "agg_time_bnds.nca")
    cfa.write(nca_fn)
    return nca_fn, files


class TestCFARead:
    def test_cfa_pure(self, testdir=TESTDIR):

        FILE = f"{testdir}/testrain.nca"

        # Local testing: Add CFAPyX before tests
        try:
            ds = xr.open_dataset(FILE, engine="CFA")
        except Exception as err:
            assert isinstance(err, ImportError)
            print("Integration tests: Read(pure) - skipped")
            return

        ## Test global dataset
        assert not hasattr(ds, "address")
        assert not hasattr(ds, "shape")
        assert not hasattr(ds, "location")

        assert "p" in ds
        assert ds["p"].shape == (20, 180, 360)

        # Using selection with lat/lon values NOT index values
        p_sel = ds["p"].isel(
            time=slice(0, 3), latitude=slice(140, 145), longitude=slice(90, 100)
        )

        assert p_sel.shape == (3, 5, 10)
        assert not hasattr(p_sel, "aggregated_data")
        assert not hasattr(p_sel, "aggregated_dimensions")
        p_mean = p_sel.mean(dim="time")

        assert p_mean.shape == (5, 10)
        assert abs(p_mean[0][0].to_numpy() - 0.635366) < 0.01, "Pure Data Invalid"

        p_value = p_sel.mean()

        assert p_value.shape == ()
        assert abs(p_value.to_numpy() - 0.511954) < 0.01, "Pure Data Invalid"

        p_indexed = ds["p"].isel(
            time=5, latitude=slice(140, 145), longitude=slice(90, 100)
        )

        assert p_indexed.shape == (5, 10), "Single Indexing Shape Failed"
        assert p_indexed.dims == ("latitude", "longitude"), (
            "Single Indexing Dims Failed"
        )

        p_data = p_indexed.to_numpy()
        assert p_data.shape == (5, 10), "Single Indexing Data Shape Failed"

        print("Integration tests: Read(pure) - complete")

    def test_lazy_time_bnds_no_fragment_fetch_on_open(self):
        """
        Opening a CFA dataset with ``time_bnds`` must not open or fetch any
        fragment files to compute ``example_value`` during CF datetime decoding.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            nca_fn, files = _build_cfa_with_time_bnds(tmpdir, calendar="standard")

            opened_fragments = []
            orig_init = NumpyDatasetHandler.__init__

            def tracking_init(handler_self, filename, *args, **kwargs):
                opened_fragments.append(filename)
                return orig_init(handler_self, filename, *args, **kwargs)

            with patch.object(NumpyDatasetHandler, "__init__", tracking_init):
                ds = xr.open_dataset(nca_fn, engine="CFA")
                _ = repr(ds)
                assert opened_fragments == [], (
                    f"Expected zero fragment opens, got {opened_fragments}"
                )

                assert np.issubdtype(ds["time"].dtype, np.datetime64)
                assert np.issubdtype(ds["time_bnds"].dtype, np.datetime64)
                assert ds["time_bnds"].shape == (6, 2)

                # Computing a middle slice of tas must not fetch time_bnds at all
                tas_mid = ds["tas"].isel(time=slice(2, 4)).compute()
                assert np.allclose(tas_mid.to_numpy(), 1.0)
                assert opened_fragments == [files[1]]

                # Computing a middle slice of time_bnds must only open frag1.nc
                opened_fragments.clear()
                tb_mid = ds["time_bnds"].isel(time=slice(2, 4)).compute()
                assert opened_fragments == [files[1]]
                expected = np.array(
                    [
                        ["2000-01-03T00:00:00", "2000-01-04T00:00:00"],
                        ["2000-01-04T00:00:00", "2000-01-05T00:00:00"],
                    ],
                    dtype="datetime64[ns]",
                )
                assert np.array_equal(tb_mid.to_numpy(), expected)
                ds.close()

    def test_lazy_time_bnds_unreachable_remote_uris(self):
        """
        Even if ``time_bnds`` points to unreachable remote URLs, opening the CFA
        dataset and reading other variables succeeds without network access.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            nca_fn, files = _build_cfa_with_time_bnds(tmpdir, calendar="standard")

            # Rewrite time_bnds fragment URIs (fragment_uris_0) to unreachable URLs
            with netCDF4.Dataset(nca_fn, "a") as nc:
                bnds_agg = nc.variables["time_bnds"].aggregated_data
                uris_var_name = dict(
                    zip(bnds_agg.split()[0::2], bnds_agg.split()[1::2])
                )["uris:"]
                uris_var = nc.variables[uris_var_name]
                for idx in np.ndindex(uris_var.shape):
                    uris_var[idx] = f"https://unreachable.invalid/frag{idx[0]}.nc"

            ds = xr.open_dataset(nca_fn, engine="CFA")
            assert ds["time_bnds"].dtype == np.dtype("datetime64[ns]")
            tas_val = ds["tas"].isel(time=0, lat=0).compute()
            assert float(tas_val.to_numpy()) == 0.0
            ds.close()

    def test_lazy_time_bnds_cftime_and_out_of_bounds_ranges(self):
        """
        Non-standard calendars (e.g. ``360_day``), ``use_cftime=True``, and
        out-of-bounds standard dates (e.g. years 1850-2300) infer ``object``
        dtype from ``time`` without fetching fragments.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            nca_360, files_360 = _build_cfa_with_time_bnds(
                tmpdir, calendar="360_day", time_units="days since 2000-01-01"
            )
            with patch.object(
                NumpyDatasetHandler,
                "__init__",
                side_effect=AssertionError("Fragment must not be opened"),
            ):
                ds_360 = xr.open_dataset(nca_360, engine="CFA")
                assert ds_360["time"].dtype == np.dtype("object")
                assert ds_360["time_bnds"].dtype == np.dtype("object")

            tb_val = ds_360["time_bnds"].isel(time=0).compute().to_numpy()
            assert tb_val[0] == cftime.Datetime360Day(2000, 1, 1, 0, 0, 0)
            assert tb_val[1] == cftime.Datetime360Day(2000, 1, 2, 0, 0, 0)
            ds_360.close()

        # Standard calendar spanning 1850 to >2262 (exceeding datetime64[ns] max)
        with tempfile.TemporaryDirectory() as tmpdir:
            nca_oob, _ = _build_cfa_with_time_bnds(
                tmpdir,
                calendar="proleptic_gregorian",
                time_units="days since 1850-01-01",
                start_offsets=[0.0, 50000.0, 165000.0],
            )
            with patch.object(
                NumpyDatasetHandler,
                "__init__",
                side_effect=AssertionError("Fragment must not be opened"),
            ):
                with pytest.warns(xr.SerializationWarning):
                    ds_oob = xr.open_dataset(nca_oob, engine="CFA")
                assert ds_oob["time"].dtype == np.dtype("object")
                assert ds_oob["time_bnds"].dtype == np.dtype("object")
                ds_oob.close()

    def test_lazy_time_bnds_coder_options_and_unit_mismatch(self):
        """
        Verify ``CFDatetimeCoder(time_unit=...)``, ``decode_times`` mapping,
        ``decode_times=False``, and differing units between ``time`` and ``time_bnds``.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            nca_fn, _ = _build_cfa_with_time_bnds(
                tmpdir,
                calendar="standard",
                time_units="days since 2000-01-01",
                bnds_units="hours since 2000-01-01",
            )
            with patch.object(
                NumpyDatasetHandler,
                "__init__",
                side_effect=AssertionError("Fragment must not be opened"),
            ):
                coder = xr.coders.CFDatetimeCoder(time_unit="s")
                ds_s = xr.open_dataset(nca_fn, engine="CFA", decode_times=coder)
                assert ds_s["time"].dtype == np.dtype("datetime64[s]")
                assert ds_s["time_bnds"].dtype == np.dtype("datetime64[s]")
                ds_s.close()

                ds_map = xr.open_dataset(
                    nca_fn,
                    engine="CFA",
                    decode_times={"time": True, "time_bnds": False},
                )
                assert np.issubdtype(ds_map["time"].dtype, np.datetime64)
                assert ds_map["time_bnds"].dtype == np.dtype("float64")
                ds_map.close()

                ds_raw = xr.open_dataset(nca_fn, engine="CFA", decode_times=False)
                assert ds_raw["time"].dtype == np.dtype("float64")
                assert ds_raw["time_bnds"].dtype == np.dtype("float64")
                ds_raw.close()

            # Also verify actual computed values when units differ (days vs hours)
            ds_check = xr.open_dataset(nca_fn, engine="CFA")
            tb0 = ds_check["time_bnds"].isel(time=0).compute().to_numpy()
            assert np.array_equal(
                tb0,
                np.array(
                    ["2000-01-01T00:00:00", "2000-01-02T00:00:00"],
                    dtype="datetime64[ns]",
                ),
            )
            ds_check.close()


if __name__ == "__main__":
    # import os
    # os.chdir('CFAPyX')

    TestCFARead().test_cfa_pure()

    print("Run with poetry run pytest -v -s")
