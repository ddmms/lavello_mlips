# -*- coding: utf-8 -*-
# Author; alin m elena, alin@elena.re
# Contribs;
# Date: 21-03-2025
# ©alin m elena, GPL v3 https://www.gnu.org/licenses/gpl-3.0.en.html
import yaml
import numpy as np
import matplotlib.pyplot as plt
import h5py
import lzma
import argparse
from pathlib import Path
import pandas as pd
import altair as alt
from sklearn.metrics import root_mean_squared_error as rmse


def plot_phonon_bands_altair(
    data_list,
    ref_data,
    k_points,
    seg_labels,
    seg_tick,
    bands,
    ml_labels,
    ref_label_text,
    title,
    fmin,
    fmax,
    save_file,
    unit_label="THz",
):
    rows = []
    npa = len(seg_labels)

    # Process ML data
    for idx, d in enumerate(data_list):
        frequencies = d["frequencies"]
        num_modes = d["num_modes"]
        label = ml_labels[idx] if ml_labels is not None else Path(bands[idx]).stem

        srmse = ""
        if "rmse" in d:
            srmse = f" (RMSE: {d['rmse']:.4f})"

        full_label = f"{label}{srmse}"

        for i in range(npa):
            start, end = seg_tick[i][0], seg_tick[i][-1]
            for mode in range(num_modes):
                for k_idx in range(start, end):
                    rows.append(
                        {
                            "k": k_points[k_idx],
                            "frequency": frequencies[k_idx, mode],
                            "mode": mode,
                            "segment": i,
                            "source": full_label,
                        }
                    )

    # Process DFT data
    if ref_data is not None:
        ref_frequencies = ref_data["frequencies"]
        num_modes = ref_data["num_modes"]
        for i in range(npa):
            start, end = seg_tick[i][0], seg_tick[i][-1]
            for mode in range(num_modes):
                for k_idx in range(start, end):
                    rows.append(
                        {
                            "k": k_points[k_idx],
                            "frequency": ref_frequencies[k_idx, mode],
                            "mode": mode,
                            "segment": i,
                            "source": ref_label_text,
                        }
                    )

    df = pd.DataFrame(rows)

    alt.data_transformers.disable_max_rows()

    # Base chart
    base = (
        alt.Chart(df)
        .mark_line(opacity=0.5, strokeWidth=2, clip=True)
        .encode(
            x=alt.X("k:Q", scale=alt.Scale(nice=False)),
            y=alt.Y("frequency:Q", title=f"Frequency [{unit_label}]"),
            color=alt.Color("source:N", scale=alt.Scale(scheme="category10")),
            detail="mode:N",
        )
        .properties(height=400)
    )

    if fmin is not None and fmax is not None:
        base = base.encode(
            y=alt.Y(
                "frequency:Q",
                scale=alt.Scale(domain=[fmin, fmax]),
                title=f"Frequency [{unit_label}]",
            )
        )

    # Facet by segment
    charts = []
    for i in range(npa):
        # Filter data for this segment to get custom ticks
        segment_ticks = seg_tick[i]
        segment_labels = seg_labels[i]

        segment_width = 400

        # Fix the expression to be a valid ternary chain, using absolute difference for robustness
        expr = ""
        for t, l in zip(segment_ticks, segment_labels):
            expr += f"abs(datum.value - {t}) < 1e-5 ? '{l}' : "
        expr += "datum.label"

        segment_chart = (
            base.transform_filter(alt.datum.segment == i)
            .encode(
                x=alt.X(
                    "k:Q",
                    axis=alt.Axis(
                        values=segment_ticks, labelExpr=expr, labelFontSize=16
                    ),
                    scale=alt.Scale(
                        domain=[k_points[segment_ticks[0]], segment_ticks[-1]],
                        nice=False,
                    ),
                    title=None,
                )
            )
            .properties(width=segment_width)
        )

        if i > 0:
            if fmin is not None and fmax is not None:
                segment_chart = segment_chart.encode(
                    y=alt.Y(
                        "frequency:Q", scale=alt.Scale(domain=[fmin, fmax]), axis=None
                    )
                )
            else:
                segment_chart = segment_chart.encode(y=alt.Y("frequency:Q", axis=None))

        charts.append(segment_chart)

    final_chart = (
        alt.hconcat(*charts)
        .properties(title=title)
        .configure_title(fontSize=20, anchor="middle")
        .configure_axis(labelFontSize=14, titleFontSize=16)
    )

    if fmin is None or fmax is None:
        final_chart = final_chart.resolve_scale(y="shared")

    if save_file:
        if not save_file.endswith(".html"):
            save_file += ".html"
        final_chart.save(save_file)
        print(f"Plot saved to {save_file}")
    else:
        # Altair doesn't have a direct 'show' like plt.show() that works everywhere,
        # but in many environments it just works if returned or saved to html and opened.
        # For CLI usage, saving to a temp file and opening might be better, but let's just save to 'phonon_bands.html' by default if no save is provided?
        # Actually, let's just save to phonon_bands.html if no save file is provided but altair is requested.
        out = "phonon_bands.html"
        final_chart.save(out)
        print(f"No save file provided. Plot saved to {out}")


def main():
    # Parse arguments:
    parser = argparse.ArgumentParser(description="distributions")
    parser.add_argument(
        "--bands",
        nargs="+",
        help="input bands files, output from some calculations",
    )
    parser.add_argument(
        "--title",
        default="xxx",
        help="title for the graph",
    )

    parser.add_argument(
        "--fmin",
        type=float,
        help="min frequency",
    )

    parser.add_argument(
        "--fmax",
        type=float,
        help="max frequency",
    )

    parser.add_argument(
        "--ref", help="input reference bands file for comparison", default=None
    )
    parser.add_argument(
        "--ml_labels", nargs="+", help="labels for ml bands", default=None
    )
    parser.add_argument("--ref_label", help="label for reference bands", default="DFT")
    parser.add_argument("--save", help="File to save the plot", default=None)
    parser.add_argument(
        "--altair",
        action="store_true",
        help="Use altair for graphs rather than matplotlib.",
    )
    parser.add_argument(
        "--cm-1",
        action="store_true",
        help="Convert frequencies to cm-1",
    )
    args = parser.parse_args()

    title = args.title
    save_file = args.save
    bands = args.bands
    fmin = args.fmin
    fmax = args.fmax
    ref_file = args.ref
    ml_labels = args.ml_labels
    ref_label_text = args.ref_label
    use_altair = args.altair
    use_cm1 = args.cm_1

    assert bands is not None and len(bands) > 0
    if ml_labels is not None:
        assert len(ml_labels) == len(bands), (
            "Number of labels must match number of band files"
        )

    data_list = []
    nqpoint = None
    labels = None
    sp = None

    for band_file in bands:
        p = Path(band_file)
        assert p.exists(), f"File {band_file} does not exist"

        ext = p.suffix
        data = None
        if ext == ".xz":
            with lzma.open(p, "r") as file:
                dc = file.read()
                data = yaml.safe_load(dc)
        elif ext == ".hdf5":
            data = h5py.File(p, "r")
        else:
            with open(p, "r") as file:
                data = yaml.safe_load(file)

        if ext == ".hdf5":
            if nqpoint is None:
                nqpoint = data["nqpoint"][:][0]
                labels = [
                    [y.decode("utf-8") for y in list(x)] for x in data["label"][:]
                ]
                sp = data["segment_nqpoint"][:][0]
            num_modes = data["natom"][()] * 3
            f = data["frequency"][:]
            frequencies = f.reshape(-1, f.shape[-1])
        else:
            if nqpoint is None:
                nqpoint = data["nqpoint"]
                labels = data["labels"]
                sp = data["segment_nqpoint"][0]
            num_modes = data["natom"] * 3
            frequencies = np.array(
                [
                    [band["frequency"] for band in phonon["band"]]
                    for phonon in data["phonon"]
                ]
            )

        data_list.append({"frequencies": frequencies, "num_modes": num_modes})

    ref_data = None
    if ref_file:
        p = Path(ref_file)
        assert p.exists(), f"Reference file {ref_file} does not exist"
        ext = p.suffix
        if ext == ".xz":
            with lzma.open(p, "r") as file:
                dc = file.read()
                data = yaml.safe_load(dc)
        elif ext == ".hdf5":
            data = h5py.File(p, "r")
        else:
            with open(p, "r") as file:
                data = yaml.safe_load(file)

        if ext == ".hdf5":
            f = data["frequency"][:]
            ref_frequencies = f.reshape(-1, f.shape[-1])
            num_modes = data["natom"][()] * 3
        else:
            ref_frequencies = np.array(
                [
                    [band["frequency"] for band in phonon["band"]]
                    for phonon in data["phonon"]
                ]
            )
            num_modes = data["natom"] * 3
        ref_data = {"frequencies": ref_frequencies, "num_modes": num_modes}

    # Unit conversion
    unit_label = "THz"
    if use_cm1:
        conversion_factor = 33.35641
        unit_label = "cm⁻¹"
        for d in data_list:
            d["frequencies"] *= conversion_factor
        if ref_data is not None:
            ref_data["frequencies"] *= conversion_factor

    k_points = np.arange(nqpoint)

    npa = -1
    seg_labels = {}
    seg_tick = {}
    for i, seg in enumerate(labels):
        if i > 0 and seg[0] == labels[i - 1][1]:
            seg_labels[npa] += [seg[1]]
        else:
            npa += 1
            seg_labels[npa] = seg

    for k in seg_labels:
        if k > 0:
            seg_tick[k] = [
                seg_tick[k - 1][-1] + i * sp for i in range(len(seg_labels[k]))
            ]
        else:
            seg_tick[k] = [i * sp for i in range(len(seg_labels[k]))]

    npa += 1

    if ref_data is not None:
        for idx, d in enumerate(data_list):
            val_rmse = rmse(ref_data["frequencies"], d["frequencies"])
            d["rmse"] = val_rmse
            label = ml_labels[idx] if ml_labels is not None else Path(bands[idx]).stem
            print(f"{label} RMSE: {val_rmse:.4f}")

    if use_altair:
        plot_phonon_bands_altair(
            data_list,
            ref_data,
            k_points,
            seg_labels,
            seg_tick,
            bands,
            ml_labels,
            ref_label_text,
            title,
            fmin,
            fmax,
            save_file,
            unit_label,
        )
        return

    fs = 8
    fsize = 40
    # Add constant 4 inches to height for title/legend, and 3 inches to width for massive Y-axis labels
    fig, axs = plt.subplots(
        nrows=1,
        ncols=npa,
        figsize=(npa * fs + 3, fs + 4),
        squeeze=False,
        subplot_kw=dict(box_aspect=1),
    )

    colors = plt.cm.tab10.colors

    for i in range(npa):
        for idx, d in enumerate(data_list):
            frequencies = d["frequencies"]
            num_modes = d["num_modes"]
            c = colors[idx % len(colors)]
            for mode in range(num_modes):
                label = Path(bands[idx]).stem if mode == 0 and i == 0 else None
                axs[0, i].plot(
                    k_points[seg_tick[i][0] : seg_tick[i][-1]],
                    frequencies[seg_tick[i][0] : seg_tick[i][-1], mode],
                    color=c,
                    alpha=0.5,
                    linewidth=3,
                    label=label,
                )

        if ref_data is not None:
            ref_frequencies = ref_data["frequencies"]
            num_modes = ref_data["num_modes"]
            for mode in range(num_modes):
                label = ref_label_text if mode == 0 and i == 0 else None
                axs[0, i].plot(
                    k_points[seg_tick[i][0] : seg_tick[i][-1]],
                    ref_frequencies[seg_tick[i][0] : seg_tick[i][-1], mode],
                    color="red",
                    alpha=0.5,
                    linewidth=3,
                    linestyle="--",
                    label=label,
                )

        axs[0, i].tick_params(axis="both", labelsize=fsize)
        axs[0, i].set_xticks(seg_tick[i], labels=seg_labels[i])
        if fmin is not None and fmax is not None:
            axs[0, i].set_ylim([fmin, fmax])
        axs[0, i].set_xlim(
            [
                k_points[seg_tick[i][0]],
                np.max(k_points[seg_tick[i][0] : seg_tick[i][-1]]) + 1,
            ]
        )
        if i == 0:
            axs[0, i].set_ylabel(f"Frequency [{unit_label}]", fontsize=fsize)
        else:
            axs[0, i].set_yticklabels([])

    if len(data_list) >= 1:
        total_items = len(data_list)
        for idx, d in enumerate(data_list):
            label = ml_labels[idx] if ml_labels is not None else Path(bands[idx]).stem
            c = colors[idx % len(colors)]

            srmse = ""
            if "rmse" in d:
                srmse = f" (RMSE: {d['rmse']:.4f})"

            axs[0, 0].text(
                0.0,
                1.05 + (total_items - 1 - idx) * 0.08,
                label + srmse,
                color=c,
                fontsize=fsize // 2,
                transform=axs[0, 0].transAxes,
                verticalalignment="bottom",
            )

        if ref_data is not None:
            axs[0, 0].text(
                0.0,
                1.05 + total_items * 0.08,
                ref_label_text,
                color="red",
                fontsize=fsize // 2,
                transform=axs[0, 0].transAxes,
                verticalalignment="bottom",
            )

    plt.suptitle(title, fontsize=fsize)
    plt.tight_layout(pad=1.5)
    if args.save is None:
        plt.show()
    else:
        plt.savefig(f"{save_file}", transparent=True, bbox_inches="tight")


if __name__ == "__main__":
    main()
