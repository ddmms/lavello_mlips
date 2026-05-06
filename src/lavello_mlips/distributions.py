import argparse
from pathlib import PosixPath
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import (
    r2_score as r2,
    root_mean_squared_error as rmse,
    mean_absolute_error as mae,
)
from ase.io import read
from ase.atoms import Atoms
from ase.formula import Formula
from numpy.typing import NDArray

import logging
from pandas import DataFrame, concat
import altair as alt

config = {
    "background": "transparent",
    "font_title": 16,
    "font_label": 14,
    "colour_labels": "blue",
    "colour_title": "green",
}


@dataclass
class ExtractedData:
    """A dataclass to hold all data extracted from the xyz files."""

    # Configuration labels
    labels: list[str] = field(default_factory=list)
    system_name: list[str] = field(default_factory=list)
    labels_forces: list[str] = field(default_factory=list)
    system_name_forces: list[str] = field(default_factory=list)

    # ML model data
    ml_energies: list[float] = field(default_factory=list)
    ml_energies_pa: list[float] = field(default_factory=list)
    ml_energies_pad: list[float] = field(default_factory=list)
    ml_energies_pan: list[float] = field(default_factory=list)
    ml_forces: list[NDArray] = field(default_factory=list)
    ml_stresses: list[NDArray] = field(default_factory=list)

    # Reference data
    ref_energies: list[float] = field(default_factory=list)
    ref_energies_pa: list[float] = field(default_factory=list)
    ref_energies_pan: list[float] = field(default_factory=list)
    ref_forces: list[NDArray] = field(default_factory=list)
    ref_stresses: list[NDArray] = field(default_factory=list)

    # Descriptor data
    desc_system: list[float] = field(default_factory=list)
    desc_per_species: dict[str, list[float]] = field(default_factory=dict)
    desc_per_atom: dict[str, list[float]] = field(default_factory=dict)

    # e0s
    e0s: dict[str, tuple[float]] = field(default_factory=dict)
    index: list[int] = field(default_factory=list)
    index_forces: list[int] = field(default_factory=list)

    def concatenate_arrays(self):
        """Concatenates lists of arrays into single numpy arrays."""
        self.ml_forces = (
            np.concatenate(self.ml_forces) if self.ml_forces else np.array([])
        )
        self.ref_forces = (
            np.concatenate(self.ref_forces) if self.ref_forces else np.array([])
        )
        self.ml_stresses = (
            np.concatenate(self.ml_stresses) if self.ml_stresses else np.array([])
        )
        self.ref_stresses = (
            np.concatenate(self.ref_stresses) if self.ref_stresses else np.array([])
        )
        self.labels_forces = (
            np.concatenate(self.labels_forces) if self.labels_forces else np.array([])
        )
        self.system_name_forces = (
            np.concatenate(self.system_name_forces)
            if self.system_name_forces
            else np.array([])
        )
        self.index_forces = (
            np.concatenate(self.index_forces) if self.index_forces else np.array([])
        )


def plot_dual_histogram_alt(
    ml: list,
    ref: list,
    ml_label: str = "ML",
    ref_label: str = "DFT",
    title: str = "None",
    xlabel: str = "None",
    nbins_max: int = 50,
):
    x_pd = DataFrame({"v": ml})
    y_pd = DataFrame({"v": ref})
    x_pd["group"] = ml_label
    y_pd["group"] = ref_label
    data_pd = concat([x_pd, y_pd])
    mxbins = get_optimal_bins(ml)
    mybins = get_optimal_bins(ref)

    mbins = max(mxbins, mybins) if max(mxbins, mybins) < 50 else nbins_max
    if mbins == 1:
        mbins = 2
    histogram = (
        alt.Chart(data_pd)
        .mark_bar(opacity=0.7)
        .encode(
            x=alt.X("v:Q", bin=alt.Bin(maxbins=mbins), title=f"{xlabel}"),
            y=alt.Y("count()", title="count", stack=None),
            color=alt.Color("group:N", legend=alt.Legend(title="Theory")),
        )
        .properties(width=300, title=alt.Title(f"{title}", subtitle=f"bins = {mbins}"))
    )
    return histogram


def plot_histogram_desc_alt(
    x: list | dict,
    title: str = "None",
    labels: list = None,
    xlabel: str = "None",
    nbins_max: int = 50,
    legend: str = "Colours",
):
    if isinstance(x, dict):
        rows = []
        for group, values in x.items():
            for v in values:
                rows.append({"x": v, "group": group})

        x_pd = DataFrame(rows)
    else:
        x_pd = DataFrame({"x": x, "group": labels})
    mxbins = get_optimal_bins(x_pd["x"])

    lab = set(x_pd["group"])
    mbins = mxbins if mxbins < 50 else nbins_max
    if mbins == 1:
        mbins = 2
    histogram = (
        alt.Chart(x_pd)
        .mark_bar(opacity=0.7)
        .encode(
            x=alt.X("x:Q", bin=alt.Bin(maxbins=mbins), title=f"{xlabel}"),
            y=alt.Y("count()", title="count", stack=None),
            color=alt.Color(
                "group:N",
                legend=alt.Legend(title=f"{legend}", symbolLimit=15)
                if len(lab) > 1
                else None,
            ),
        )
        .properties(title=alt.Title(f"{title}", subtitle=f"bins = {mbins}"))
    )
    return histogram


def plot_parity_alt(
    x: list,
    y: list,
    labels: list,
    indices: list,
    title: str,
    xlabel: str,
    ylabel: str,
    unit: str,
    logger: logging.Logger = None,
):
    val_r2 = r2(x, y)
    val_rmse = rmse(x, y)
    val_mae = mae(x, y)

    data_pd = DataFrame({"x": x, "y": y, "category": labels, "index": indices})
    xmin = np.min(x)
    xmax = np.max(x)
    ymin = np.min(y)
    ymax = np.max(y)

    min_val = min(xmin, ymin)
    max_val = max(xmax, ymax)
    line_data = DataFrame({"x": [min_val, max_val], "y": [min_val, max_val]})
    base = (
        alt.Chart(data_pd)
        .mark_point(opacity=0.7)
        .encode(
            x=alt.X(
                "x",
                scale=alt.Scale(domain=[min_val, max_val]),
                axis=alt.Axis(title=f"{xlabel} [{unit}]"),
            ),
            y=alt.Y(
                "y",
                scale=alt.Scale(domain=[min_val, max_val]),
                axis=alt.Axis(title=f"{ylabel} [{unit}]"),
            ),
            color=alt.Color(
                "category:N", legend=alt.Legend(title="Colours", symbolLimit=15)
            ),
            tooltip=["x", "y", "category", "index"],
        )
        .properties(width=300, height=300)
    )

    line = (
        alt.Chart(line_data)
        .mark_line(color="red", strokeDash=[5, 5])
        .encode(x=alt.X("x:Q"), y=alt.Y("y:Q"))
        .properties(width=300, height=300)
    )
    er = f"r²={val_r2:.4f} RMSE={val_rmse * 1000:.4f} [m{unit}] MAE={val_mae * 1000:.4f} [m{unit}]"
    if logger:
        logger.info(f"{er} {title}")
    else:
        print(f"{er} {title}")

    scatter_plot = (base + line).properties(
        title=alt.Title(f"{title}", subtitle=f"{er}"),
        width=300,
        height=300,
    )
    # data pd is optional, allow some reactivity in the graph

    return scatter_plot


def set_tags(ml_tag: str = "mace_mp", ref_tag: str = "dft") -> dict:
    """Generate property tags"""
    tags = {
        p: {m: f"{t}_{p}" for m, t in [("ml", ml_tag), ("ref", ref_tag)]}
        for p in ["energy", "forces", "stress"]
    }
    tags["ml_tag"] = ml_tag
    tags["ref_tag"] = ref_tag
    return tags


def add_identity_line(ax, **line_kwargs):
    """Adds a y=x identity line to a matplotlib axes object."""
    (identity,) = ax.plot([], [], **line_kwargs)

    def callback(axes):
        low_x, high_x = axes.get_xlim()
        low_y, high_y = axes.get_ylim()
        low = max(low_x, low_y)
        high = min(high_x, high_y)
        identity.set_data([low, high], [low, high])

    callback(ax)
    ax.callbacks.connect("xlim_changed", callback)
    ax.callbacks.connect("ylim_changed", callback)


def get_optimal_bins(data: np.ndarray) -> int:
    """Calculates the optimal number of bins for a histogram using Freedman-Diaconis rule."""
    if len(data) < 2:
        return 1
    q25, q75 = np.percentile(data, [25, 75])
    iqr = q75 - q25
    bin_width = 2 * iqr * len(data) ** (-1 / 3)
    if bin_width == 0:
        return 50
    bins = round((max(data) - min(data)) / bin_width)
    return max(1, int(bins))


def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate parity plots and histograms for ML model predictions."
    )
    parser.add_argument(
        "--xyz", "-x", required=True, help="Input XYZ file with calculation results."
    )
    parser.add_argument(
        "--e0s", "-e", help="XYZ file with single-atom reference energies (e0s)."
    )
    parser.add_argument(
        "--ml_tag", "-m", default="mace_mp", help="Tag for ML model properties."
    )
    parser.add_argument(
        "--ref_tag", "-r", default="dft", help="Tag for reference model properties."
    )
    parser.add_argument("--title", "-t", help="Custom title for the plot.")
    parser.add_argument(
        "--save", "-s", help="File to save the plot (without extension)."
    )
    parser.add_argument(
        "--descriptor-scale-factor",
        "-d",
        type=float,
        default=1.0e5,
        help="Scaling factor to apply to descriptor values for plotting.",
    )
    parser.add_argument(
        "-a",
        "--altair",
        action="store_true",
        help="Use altair for graphs rather than matplotlib.",
    )
    parser.add_argument(
        "-n",
        "--name",
        action="store_true",
        help="Use system_name for categories in parity plots.",
    )
    parser.add_argument(
        "-p",
        "--parity_plots",
        action="store_true",
        help="Do only the parity plots.",
    )
    return parser.parse_args()


def _get_voigt_stress(stress_array: list) -> list:
    """Converts a 3x3 or 9-element stress tensor to 6-element Voigt notation."""
    s = np.array(stress_array).flatten()
    if len(s) == 9:  # From 3x3 matrix flattened
        return [s[0], s[4], s[8], s[5], s[2], s[1]]  # xx, yy, zz, yz, xz, xy
    elif len(s) == 6:  # Already in Voigt
        return list(s)
    return []


def extract_isolated_atoms(
    e0s: str | list[Atoms] | PosixPath, ref_tag: str, ml_tag: str
) -> dict:
    """Loads single-atom reference energies from a file."""
    e0 = {}
    if isinstance(e0s, str) or isinstance(e0s, PosixPath):
        e0s = read(e0s, index=":")
    for atom in e0s:
        if (
            atom.info.get("config_type") == "IsolatedAtom"
            or atom.info.get("config_type") == "iso"
        ):
            symbol = atom.get_chemical_symbols()[0]
            e0_ref = atom.info.get(ref_tag)
            e0_ml = atom.info.get(ml_tag)
            e0[symbol] = (e0_ref, e0_ml)
    return e0


def extract_data(
    traj: str | list[Atoms] | PosixPath,
    E0: Optional[str | list[Atoms] | PosixPath] = None,
    descriptor_scale_factor: Optional[float] = 1.0e5,
    tags: dict = None,
) -> ExtractedData:
    """Extracts energies, forces, stresses, and descriptors from a list of ASE Atoms objects or a file."""
    data = ExtractedData()
    if isinstance(traj, str) or isinstance(traj, PosixPath):
        frames = read(traj, index=":")
    else:
        frames = traj

    if E0:
        data.e0s = extract_isolated_atoms(
            E0, ref_tag=tags["energy"]["ref"], ml_tag=tags["energy"]["ml"]
        )

    for i, f in enumerate(frames):
        n_atoms = len(f)
        label = f.info.get("config_type", "None")
        sn = f.info.get("system_name", "None")

        # --- Extract ML and Reference Data ---
        ml_energy = f.info.get(tags["energy"]["ml"])
        ref_energy = f.info.get(tags["energy"]["ref"])
        data.index.append(i)

        if ml_energy is not None:
            data.ml_energies.append(ml_energy)
            if f.has(tags["forces"]["ml"]):
                data.ml_forces.append(f.get_array(tags["forces"]["ml"]).flatten())
                data.labels_forces.append([label] * (3 * n_atoms))
                data.system_name_forces.append([sn] * (3 * n_atoms))
                data.index_forces.append([i] * (3 * n_atoms))
            if tags["stress"]["ml"] in f.info:
                data.ml_stresses.append(_get_voigt_stress(f.info[tags["stress"]["ml"]]))

        if ref_energy is not None:
            data.ref_energies.append(ref_energy)
            if f.has(tags["forces"]["ref"]):
                data.ref_forces.append(f.get_array(tags["forces"]["ref"]).flatten())
            if tags["stress"]["ref"] in f.info:
                data.ref_stresses.append(
                    _get_voigt_stress(f.info[tags["stress"]["ref"]])
                )

        if ml_energy is not None or ref_energy is not None:
            data.labels.append(label)
            data.system_name.append(sn)

        # --- Calculate and store energy per atom and formation energy if e0s are available ---
        e0_ref_total = 0.0
        e0_ml_total = 0.0
        if data.e0s:
            composition = Formula(f.get_chemical_formula()).count()
            e0_ref_total = sum(
                composition[k] * data.e0s[k][0]
                for k in composition
                if k in data.e0s and data.e0s[k][0] is not None
            )
            e0_ml_total = sum(
                composition[k] * data.e0s[k][1]
                for k in composition
                if k in data.e0s and data.e0s[k][1] is not None
            )

        if ref_energy is not None and e0_ref_total:
            data.ref_energies_pa.append((ref_energy - e0_ref_total) / n_atoms)
        if ref_energy is not None:
            data.ref_energies_pan.append((ref_energy) / n_atoms)
        if ml_energy is not None and e0_ml_total:
            data.ml_energies_pa.append((ml_energy - e0_ml_total) / n_atoms)
        if ml_energy is not None and e0_ref_total:
            data.ml_energies_pad.append((ml_energy - e0_ref_total) / n_atoms)
        if ml_energy is not None:
            data.ml_energies_pan.append((ml_energy) / n_atoms)

        # --- Extract Descriptors ---
        if f"{tags['ml_tag']}_descriptor" in f.info:
            data.desc_system.append(
                f.info[f"{tags['ml_tag']}_descriptor"] * descriptor_scale_factor
            )
        if f"{tags['ml_tag']}_descriptors" in f.arrays:
            for i_atom, atom in enumerate(f):
                data.desc_per_atom.setdefault(atom.symbol, []).append(
                    f.arrays[f"{tags['ml_tag']}_descriptors"][i_atom]
                    * descriptor_scale_factor
                )

        specs = set(f.symbols)
        ps = [f"{tags['ml_tag']}_{x}_descriptor" for x in specs]
        for d, m in zip(ps, specs):
            if d in f.info:
                data.desc_per_species.setdefault(m, []).append(
                    f.info[d] * descriptor_scale_factor
                )

    data.concatenate_arrays()
    return data


def plot_parity(ax, ref_data, ml_data, labels, colors, title, unit, name):
    """Creates a single parity scatter plot on a given axes object."""
    ax.set_title(title)
    ax.set_xlabel(f"Reference ({name}) [{unit}]")
    ax.set_ylabel(f"ML ({name}) [{unit}]")

    for cat, color in colors.items():
        mask = np.array(labels) == cat
        ax.scatter(
            np.asarray(ref_data)[mask],
            np.asarray(ml_data)[mask],
            label=cat,
            color=color,
            alpha=0.7,
            s=20,
        )

    val_r2 = r2(ref_data, ml_data)
    val_rmse = rmse(ref_data, ml_data)

    rmse_unit = "meV" if "eV" in unit else unit
    rmse_val = val_rmse * 1000 if "eV" in unit else val_rmse
    rmse_prec = ".3f" if "eV" in unit else ".4f"

    ax.annotate(f"$R^2$ = {val_r2:.4f}", (0.05, 0.95), xycoords="axes fraction")
    ax.annotate(
        f"RMSE = {rmse_val:{rmse_prec}} {rmse_unit}",
        (0.05, 0.88),
        xycoords="axes fraction",
    )

    ax.legend()
    ax.set_aspect("equal", "box")
    add_identity_line(ax, color="k", ls="--", lw=1.5, label="_nolegend_")


def plot_histogram(ax, data, title, xlabel, nbin_fallback=50):
    """Creates a histogram plot for one or two datasets."""
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Count")
    k = 0
    for label, d in data.items():
        if len(d) > 0:
            bins = get_optimal_bins(d) if len(d) > 1 else nbin_fallback
            ax.hist(d, bins=bins, label=label if k < 10 else None, alpha=0.7)
            k += 1
    if len(data) > 1:
        ax.legend()


def extract_and_plot(
    xyz_path: str | list[Atoms] | PosixPath,
    e0s_path: Optional[str | list[Atoms] | PosixPath] = None,
    save_path: Optional[str] = None,
    ml_tag: str = "mace_mp",
    ref_tag: str = "dft",
    title: Optional[str] = None,
    descriptor_scale_factor: float = 1.0e5,
    use_altair: bool = False,
    use_system_name: bool = False,
    parity_plots_only: bool = False,
    logger=None,
):
    """
    Runs the full analysis and plotting pipeline.

    This function extracts data from XYZ files, processes it, and generates
    parity plots and histograms to compare ML model predictions against
    reference data. It can be called directly from other Python scripts.

    Args:
        xyz_path: Path to the input XYZ file with calculation results.
        e0s_path: Optional path to the XYZ file with single-atom reference energies.
        save_path: Optional file path to save the plot (without extension).
        ml_tag: Tag for ML model properties.
        ref_tag: Tag for reference model properties.
        title: Optional custom title for the plot.
        descriptor_scale_factor: Scaling factor for descriptor values.
        use_altair: If True, use Altair for interactive plots. Otherwise, use Matplotlib.
        use_system_name: If True, use 'system_name' for categories in parity plots.
        parity_plots_only: If True, only generate parity plots.
    """
    tags = set_tags(ml_tag, ref_tag)
    data = extract_data(
        traj=xyz_path,
        E0=e0s_path,
        descriptor_scale_factor=descriptor_scale_factor,
        tags=tags,
    )

    categories = sorted(list(set(data.labels)))
    colors = {cat: c for cat, c in zip(categories, plt.cm.tab10.colors)}

    plot_sections = {
        "parity": bool(data.ref_energies and data.ml_energies),
        "histograms": bool(data.ref_energies or data.ml_energies),
        "descriptors": bool(
            data.desc_system or data.desc_per_atom or data.desc_per_species
        ),
    }
    if parity_plots_only:
        plot_sections["histograms"] = False
        plot_sections["descriptors"] = False

    plot_defs = [
        (
            "Energies",
            "eV",
            data.ref_energies,
            data.ml_energies,
            data.system_name if use_system_name else data.labels,
            data.index,
        ),
        (
            "Energy/atom",
            "eV/atom",
            data.ref_energies_pan,
            data.ml_energies_pan,
            data.system_name if use_system_name else data.labels,
            data.index,
        ),
        (
            "Formation energy/atom",
            "eV/atom",
            data.ref_energies_pa,
            data.ml_energies_pa,
            data.system_name if use_system_name else data.labels,
            data.index,
        ),
        (
            "Formation energy/atom - DFT ref",
            "eV/atom",
            data.ref_energies_pa,
            data.ml_energies_pad,
            data.system_name if use_system_name else data.labels,
            data.index,
        ),
        (
            "Forces",
            "eV/Å",
            data.ref_forces,
            data.ml_forces,
            data.system_name_forces if use_system_name else data.labels_forces,
            data.index_forces,
        ),
        (
            "Stresses (Voigt)",
            "eV/Å³",
            data.ref_stresses,
            data.ml_stresses,
            np.repeat(data.system_name if use_system_name else data.labels, 6),
            np.repeat(data.index, 6),
        ),
    ]
    hist_defs = [
        (
            "Energy Distribution",
            "Energy [eV]",
            {"ML": data.ml_energies, "Ref": data.ref_energies},
        ),
        (
            "Energy/atom Distribution",
            "Energy/atom [eV]",
            {"ML": data.ml_energies_pan, "Ref": data.ref_energies_pan},
        ),
        (
            "Formation Energy/atom Distribution",
            "Formation energy/atom [eV]",
            {"ML": data.ml_energies_pa, "Ref": data.ref_energies_pa},
        ),
        (
            "Formation Energy/atom Distribution DFT E0s",
            "Formation energy/atom [eV]",
            {"ML": data.ml_energies_pad, "Ref": data.ref_energies_pa},
        ),
        (
            "Force Component Distribution",
            "Force [eV/Å]",
            {"ML": data.ml_forces, "Ref": data.ref_forces},
        ),
        (
            "Stress Component Distribution",
            "Stress [eV/Å³]",
            {"ML": data.ml_stresses, "Ref": data.ref_stresses},
        ),
    ]
    desc_defs = [
        (data.desc_system, "descriptors [a.u.]", "System Descriptors", "System"),
        (data.desc_per_species, "descriptors [a.u.]", "Species Descriptors", "Species"),
        (data.desc_per_atom, "descriptors [a.u.]", "Atomic Descriptors", "Atomic"),
    ]
    num_rows = sum(plot_sections.values())
    num_cols = max(len(plot_defs), len(hist_defs), 3)

    if num_rows == 0:
        if logger:
            logger.info("No data available to plot.")
        else:
            print("No data available to plot.")
        return

    if use_altair:
        all_graphs = []
        ppE = []
        if plot_sections["parity"]:
            for name, unit, ref_d, ml_d, lbls, inds in plot_defs:
                if len(ref_d) > 0 and len(ref_d) == len(ml_d):
                    ppE.append(
                        plot_parity_alt(
                            x=ml_d,
                            y=ref_d,
                            labels=lbls,
                            title=name,
                            xlabel=f"{tags['ml_tag']} ",
                            ylabel=f"{tags['ref_tag']} ",
                            indices=inds,
                            unit=unit,
                            logger=logger,
                        )
                    )
            pE_raw = alt.hconcat(*ppE)
            all_graphs.append(pE_raw)

        if plot_sections["histograms"]:
            hE = []
            for i, (hist_title, xlabel, hist_data) in enumerate(hist_defs):
                if any(len(d) > 0 for d in hist_data.values()):
                    hE.append(
                        plot_dual_histogram_alt(
                            ml=hist_data["ML"],
                            ref=hist_data["Ref"],
                            ml_label=tags["ml_tag"],
                            ref_label=tags["ref_tag"],
                            title=hist_title,
                            xlabel=xlabel,
                        )
                    )
            hE_raw = alt.hconcat(*hE)
            all_graphs.append(hE_raw)

        dp = []
        if plot_sections["descriptors"]:
            for i, (d, xlabel, desc_title, legend) in enumerate(desc_defs):
                dp.append(
                    plot_histogram_desc_alt(
                        x=d, xlabel=xlabel, title=desc_title, legend=legend
                    )
                )
            desc_raw = alt.hconcat(*dp).resolve_scale(color="independent")
            all_graphs.append(desc_raw)

        final = (
            alt.vconcat(*all_graphs)
            .resolve_scale(color="independent")
            .properties(background=config["background"])
            .configure_title(
                fontSize=config["font_title"], color=config["colour_title"]
            )
            .configure_axis(
                labelColor=config["colour_labels"],
                titleColor=config["colour_labels"],
                labelFontSize=config["font_label"],
            )
        )
        if save_path:
            final.save(f"{save_path}.html", inline=True)
            # final.save(f"{save_path}.png", inline=True)
            # final.save(f"{save_path}.svg", transparent=True)
            if logger:
                logger.info(f"Plot saved to {save_path}.html/png")
            else:
                print(f"Plot saved to {save_path}.html/png")

    else:  # Use Matplotlib
        fig, axs = plt.subplots(
            nrows=num_rows,
            ncols=num_cols,
            figsize=(16, num_cols * num_rows),
            squeeze=False,
            constrained_layout=True,
        )
        plot_title = (
            title or f"Parity Plots & Histograms: {ref_tag.upper()} vs {ml_tag.upper()}"
        )
        fig.suptitle(plot_title, fontsize=16)

        current_row = 0
        if plot_sections["parity"]:
            for i, (name, unit, ref_d, ml_d, lbls, _) in enumerate(plot_defs):
                if len(ref_d) > 0 and len(ref_d) == len(ml_d):
                    plot_parity(
                        axs[current_row, i],
                        ref_d,
                        ml_d,
                        lbls,
                        colors,
                        name,
                        unit,
                        "Data",
                    )
                else:
                    axs[current_row, i].axis("off")
            current_row += 1

        if plot_sections["histograms"]:
            for i, (hist_title, xlabel, hist_data) in enumerate(hist_defs):
                if any(len(d) > 0 for d in hist_data.values()):
                    plot_histogram(axs[current_row, i], hist_data, hist_title, xlabel)
                else:
                    axs[current_row, i].axis("off")
            current_row += 1

        if plot_sections["descriptors"]:
            nc = 0
            if data.desc_system:
                plot_histogram(
                    axs[current_row, nc],
                    {"System": data.desc_system},
                    "System Descriptors",
                    "Descriptor Value",
                )
            if data.desc_per_species:
                nc += 1
                plot_histogram(
                    axs[current_row, nc],
                    data.desc_per_species,
                    "Per Species Descriptors",
                    "Descriptor Value",
                )
            if data.desc_per_atom:
                nc += 1
                plot_histogram(
                    axs[current_row, nc],
                    data.desc_per_atom,
                    "Per-Atom Descriptors",
                    "Descriptor Value",
                )
            for i in range(nc + 1, num_cols):
                axs[current_row, i].axis("off")

        if save_path:
            plt.savefig(
                f"{save_path}.png", transparent=True, dpi=300, bbox_inches="tight"
            )
            plt.savefig(f"{save_path}.svg", transparent=True, bbox_inches="tight")
            print(f"Plot saved to {save_path}.png/.svg")
        else:
            plt.show()


def main():
    """Parses command-line arguments and runs the analysis."""
    args = parse_arguments()
    extract_and_plot(
        xyz_path=args.xyz,
        e0s_path=args.e0s,
        save_path=args.save,
        ml_tag=args.ml_tag,
        ref_tag=args.ref_tag,
        title=args.title,
        descriptor_scale_factor=args.descriptor_scale_factor,
        use_altair=args.altair,
        use_system_name=args.name,
        parity_plots_only=args.parity_plots,
    )


if __name__ == "__main__":
    main()
