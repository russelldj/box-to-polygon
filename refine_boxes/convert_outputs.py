import argparse
import glob
import pdb
from os.path import basename, join, splitext

import kwcoco
import ubelt as ub

from refine_boxes.utils import get_all_files

INPUT_DIR = "/home/local/KHQ/david.russell/experiments/NOAA/sealion_pixel/output"
BASENAME = "utility_add_segmentations_"
METHOD = "grabcut"
OUTPUT_DIR = (
    "/home/local/KHQ/david.russell/data/viame_dvc/public/Aerial/US_ALASKA_MML_SEALION"
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default=INPUT_DIR)
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    parser.add_argument("--method", default=METHOD)
    parser.add_argument("--dvc", action="store_true")
    parser.add_argument("--convert", action="store_true")
    args = parser.parse_args()
    return args


def convert(in_path, out_path):
    from bioharn.io.viame_csv import ViameCSV

    dset = kwcoco.CocoDataset()

    # TODO: ability to map image ids to agree with another coco file
    csv = ViameCSV(in_path)
    csv.extend_coco(dset=dset)
    dset.dump(out_path, newlines=True)


if __name__ == "__main__":
    args = parse_args()
    files = glob.glob(join(args.input_dir, BASENAME + args.method + "*.csv"))
    print(ub.repr2(files))
    for f in files:
        year = splitext(basename(f))[0][len(BASENAME) + len(args.method) + 1 :]
        converted_file = f.replace(".csv", ".kwcoco.json")
        if args.convert:
            convert(f, converted_file)
        if args.dvc:
            dvc_folder = join(args.output_dir, str(year))
            dvc_files = get_all_files(dvc_folder)
            # print(ub.repr2(dvc_files))
            input_csv = list(
                filter(
                    lambda x: all(
                        s not in basename(x)
                        for s in ("watershed", "grabcut", "kwcoco", "dvc", "image")
                    ),
                    dvc_files,
                )
            )
            if len(input_csv) != 1:
                pdb.set_trace()
            input_csv = input_csv[0]
            dvc_coco_file = join(
                dvc_folder,
                basename(input_csv).split(".")[0] + "_" + args.method + ".kwcoco.json",
            )
            dvc_csv_file = join(
                dvc_folder,
                basename(input_csv).split(".")[0] + "_" + args.method + ".viame.csv",
            )

            ub.cmd(f"dvc unprotect '{dvc_coco_file}'", verbose=3)
            ub.cmd(f"cp {converted_file} {dvc_coco_file}", verbose=3)
            ub.cmd(f"dvc --cd {dvc_folder} add '{basename(dvc_coco_file)}'", verbose=3)

            ub.cmd(f"dvc unprotect '{dvc_csv_file}'", verbose=3)
            ub.cmd(f"cp {f} {dvc_csv_file}", verbose=3)
            ub.cmd(f"dvc --cd {dvc_folder} add '{basename(dvc_csv_file)}'", verbose=4)
