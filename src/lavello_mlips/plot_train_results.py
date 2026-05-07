import yaml
import argparse
from pathlib import Path
from janus_core.calculations.single_point import SinglePoint
from ase.io import read
from .distributions import extract_and_plot
import logging
from itertools import chain
from .utils import setup_logging


def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate parity plots and histograms for ML model predictions."
    )
    (
        parser.add_argument(
            "--yaml",
            "-y",
            default=None,
            help="yaml file containinng all input and outut training data, works only with params in yaml file",
        ),
    )
    parser.add_argument(
        "--device", "-d", default="cpu", help="device on which to run mlip calculations"
    )
    parser.add_argument(
        "--arch",
        "-a",
        default="mace_mp",
        help="device on which to run mlip calculations",
    )
    parser.add_argument("--model", default="", help="model to run mlip calculations")
    parser.add_argument(
        "--head", default=None, help="head of the model  run mlip calculations"
    )
    parser.add_argument(
        "--default_dtype", default="float32", help="Default precision for calculations."
    )
    parser.add_argument(
        "--ml_tag", "-m", default="mace_mp", help="Tag for ML model properties."
    )
    parser.add_argument(
        "--ref_tag", "-r", default="dft", help="Tag for reference model properties."
    )
    parser.add_argument("--title", "-t", help="Custom title for the plot.")
    parser.add_argument(
        "--log", "-l", default="plot.log", help="Custom title for the plot."
    )
    parser.add_argument(
        "--iso",
        "-i",
        default="isolated.xyz",
        help="file, containing isolated energies.",
    )
    parser.add_argument(
        "--frames", "-f", default="", help="file, containing frames to evaluate."
    )
    parser.add_argument(
        "--altair",
        action="store_true",
        help="Use altair for graphs rather than matplotlib.",
    )
    parser.add_argument(
        "-p",
        "--parity_plots",
        action="store_true",
        help="Do only the parity plots.",
    )
    parser.add_argument(
        "--stage_two",
        action="store_true",
        help="Use the stage two model.",
    )
    return parser.parse_args()
    return parser.parse_args()


def main():
    args = parse_arguments()
    yaml_file = args.yaml
    device = args.device
    arch = args.arch
    ml_tag = args.ml_tag
    ref_tag = args.ref_tag
    title = args.title
    iso = args.iso
    pp = args.parity_plots
    alt = args.altair
    model = args.model
    head = args.head
    frames = args.frames
    stage_two = args.stage_two
    default_dtype = args.default_dtype
    setup_logging(log_file_path=args.log)
    logger = logging.getLogger(__name__)
    phead = f"-{head}" if head else ""
    suf = "-stagetwo" if stage_two else ""
    model_name = model.replace(".model", "")
    if yaml_file:
        with open(yaml_file, "r") as f:
            config = yaml.safe_load(f)
        if args.stage_two:
            model = config["name"] + "_stagetwo.model"
        else:
            model = config["name"] + ".model"
        model_name = model.replace(".model", "")
        for head in config["heads"]:
            if head == "pt_head":
                logger.info(f"skip {head}")
                continue
            else:
                logger.info(f"compute {head}")
                all_files = {}
                _train = config["heads"][head]["train_file"]
                train_files = _train if isinstance(_train, list) else [_train]
                isolated = [item for item in train_files if iso in item]
                all_files["train"] = [item for item in train_files if iso not in item]
                _test = config["heads"][head]["test_file"]
                all_files["test"] = _test if isinstance(_test, list) else [_test]
                try:
                    _valid = config["heads"][head]["valid_file"]
                    all_files["valid"] = (
                        _valid if isinstance(_valid, list) else [_valid]
                    )
                except:
                    pass
                output = Path(f"janus-results-{head}{suf}")
                if len(isolated) == 0 or not Path(isolated[0]).exists():
                    raise ValueError(f"isolated atoms file does not exist {iso}")
                iso_res = Path(isolated[0]).stem + "-results.extxyz"
                calc_kwargs = {"default_dtype": default_dtype}
                if head:
                    calc_kwargs["head"] = head
                sp = SinglePoint(
                    struct=isolated[0],
                    arch=arch,
                    device=device,
                    model=model,
                    calc_kwargs=calc_kwargs,
                    enable_progress_bar=True,
                    file_prefix=output / Path(isolated[0]).stem,
                    write_results=True,
                )
                sp.run()
                all_res = {}

                for k in all_files.keys():
                    all_res[k] = []
                    for f in all_files[k]:
                        logger.info(f"singlepoint calculations on {f}")
                        t = Path(f).stem + "-results.extxyz"
                        calc_kwargs = {"default_dtype": default_dtype}
                        if head:
                            calc_kwargs["head"] = head
                        sp = SinglePoint(
                            struct=f,
                            arch=arch,
                            device=device,
                            model=model,
                            calc_kwargs=calc_kwargs,
                            enable_progress_bar=True,
                            file_prefix=output / Path(f).stem,
                            write_results=True,
                        )
                        sp.run()
                        r = read(output / t, index=":")
                        all_res[k] += r
                        logger.info(f"plotting {output / t}")
                        extract_and_plot(
                            xyz_path=r,
                            e0s_path=output / iso_res,
                            save_path=f"{Path(f).stem}-{head}-{model_name}{suf}",
                            ml_tag=ml_tag,
                            ref_tag=ref_tag,
                            title=title,
                            use_altair=alt,
                            use_system_name=False,
                            parity_plots_only=pp,
                            logger=logger,
                        )
            for k in all_res:
                logger.info(f"plotting all-{k}")
                extract_and_plot(
                    xyz_path=all_res[k],
                    e0s_path=output / iso_res,
                    save_path=f"all-{k}-{head}-{model_name}{suf}",
                    ml_tag=ml_tag,
                    ref_tag=ref_tag,
                    title="Parity plots&histograms",
                    use_altair=alt,
                    use_system_name=False,
                    parity_plots_only=pp,
                    logger=logger,
                )

            all_frames = list(
                chain(all_res["train"], all_res["test"], all_res["valid"])
            )
            extract_and_plot(
                xyz_path=all_frames,
                e0s_path=output / iso_res,
                save_path=f"all-{head}-{model_name}{suf}",
                ml_tag=ml_tag,
                ref_tag=ref_tag,
                title="Parity plots&histograms",
                use_altair=alt,
                use_system_name=False,
                parity_plots_only=pp,
                logger=logger,
            )
            extract_and_plot(
                xyz_path=all_frames,
                e0s_path=output / iso_res,
                save_path=f"all_names-{head}-{model_name}{suf}",
                ml_tag=ml_tag,
                ref_tag=ref_tag,
                title="Parity plots&histograms",
                use_altair=alt,
                use_system_name=True,
                parity_plots_only=pp,
                logger=logger,
            )
    else:
        logger.info(f"evaluate {frames}")
        calc_kwargs = {"default_dtype": default_dtype}
        if head:
            calc_kwargs["head"] = head
        sp = SinglePoint(
            struct=iso,
            arch=arch,
            device=device,
            model=model,
            calc_kwargs=calc_kwargs,
            enable_progress_bar=True,
            write_results=True,
        )
        sp.run()
        sp = SinglePoint(
            struct=frames,
            arch=arch,
            device=device,
            model=model,
            calc_kwargs=calc_kwargs,
            enable_progress_bar=True,
            write_results=True,
        )
        sp.run()
        t = Path(frames).stem + "-results.extxyz"
        r = read(output / t, index=":")
        iso_res = Path(iso).stem + "-results.extxyz"
        extract_and_plot(
            xyz_path=r,
            e0s_path=output / iso_res,
            save_path=f"{Path(frames).stem}{phead}-{model_name}{suf}",
            ml_tag=ml_tag,
            ref_tag=ref_tag,
            title=title,
            use_altair=alt,
            use_system_name=False,
            parity_plots_only=pp,
            logger=logger,
        )


if __name__ == "__main__":
    main()
