import argparse
import logging
import os
import pathlib
from glob import glob

import pandas as pd
import ubelt as ub

from refine_boxes.convert_outputs import convert
from refine_boxes.utils import ensure_containing_dir, get_all_files

CURRENT_LOCATION = os.getcwd()
EXTENSTION = ".jpg"
PIPELINE_DIR = os.path.abspath(os.path.join(CURRENT_LOCATION, "pipelines"))
OUTPUT_DIR = os.path.abspath(os.path.join(CURRENT_LOCATION, "output"))
IMAGE_LIST = os.path.abspath(os.path.join(CURRENT_LOCATION, "temp/image_list.txt"))
METHOD = "utility_add_segmentations_watershed"


def parse_args():
    """parse arguments from the command line"""
    parser = argparse.ArgumentParser(
        "Utility to run command kwiver refiner and post process the results"
    )
    parser.add_argument("--method", default=METHOD)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--pipeline-dir", default=PIPELINE_DIR)
    parser.add_argument("--validate-lengths", action="store_true")
    parser.add_argument("--check-output-counts", action="store_true")
    parser.add_argument("--folder-index", type=int)
    parser.add_argument("--image-list-file", default=IMAGE_LIST)
    args = parser.parse_args()
    return args


def count_lines(filename):
    """Count the lines in a file"""
    line_count = 0
    for _ in open(filename):
        line_count += 1
    return line_count


def fix_frame_number(input_filename):
    """Update data file to have frame number correspond to ordered filenames"""
    print(input_filename)
    os.system(f"dvc unprotect {input_filename}")
    data = pd.read_csv(input_filename, names=range(16))
    filenames = data.iloc[:, 1].copy()
    unique_filenames = filenames.unique()
    unique_filenames = unique_filenames.tolist()
    unique_filenames = pd.Index(unique_filenames)
    filename_indices = [
        unique_filenames.get_loc(filename) for filename in filenames.tolist()
    ]
    print(data)
    data.iloc[:, 2] = filename_indices
    data.dropna(how="all", axis=1, inplace=True)
    print(data)
    data.to_csv(input_filename, header=False, index=False)
    cmd_str = f"dvc --cd {os.path.dirname(input_filename)} add {os.path.basename(input_filename)}"
    print(cmd_str)
    os.system(cmd_str)


def get_annotation_file(folder):
    """Get original annotation file from a folder"""
    annotation_file = os.path.join(
        folder, "sealions_" + os.path.basename(folder) + "*.viame.csv"
    )
    annotation_file = glob(annotation_file)
    # Avoid getting computed ones
    annotation_file = list(
        filter(lambda x: "watershed" not in x and "grabcut" not in x, annotation_file)
    )
    if len(annotation_file) != 1:
        # Guess that it's one with the just annotations
        return os.path.join(folder, "annotations.csv")
    return annotation_file[0]


def validate_lengths(annotation_folder, output_folder):
    """Validate that the number of annotatations in the input and output folders match"""
    for a, output_file in zip(
        get_all_files(annotation_folder, isdir=True),
        get_all_files(output_folder, sort_key=os.path.basename),
    ):
        annotation_file = get_annotation_file(a)
        annotation_count = count_lines(annotation_file)
        output_count = count_lines(output_file)
        print(
            annotation_count - output_count,
            annotation_count,
            output_count,
            output_file,
            annotation_file,
        )


def write_image_list(annotation_file, folder, image_list_file, add_image=False):
    image_names = pd.read_csv(annotation_file, squeeze=True, usecols=[1]).unique()
    # TODO update this
    image_files = list(
        map(
            lambda x: os.path.join(folder, x)
            if not add_image or "images" in x
            else os.path.join(folder, "images", x),
            image_names,
        )
    )
    ensure_containing_dir(image_list_file)
    with open(image_list_file, "w") as fout:
        fout.write("\n".join(image_files))


def compute(
    root_dir,
    method=METHOD,
    output_dir=OUTPUT_DIR,
    pipeline_dir=PIPELINE_DIR,
    run=True,
    image_list_file=IMAGE_LIST,
    extension=EXTENSTION,
    debug=False,
    convert_only=False,
    check_output_counts=False,
    folder_index=None,
    fix_frame=False,
    is_sealion=False,
):
    """Do a variety of processing"""

    # Get folders in this directory
    folders = get_all_files(root_dir, isdir=True)
    logging.warning(f"Folders {folders}")
    # Allow a subset of indices to be specified
    if folder_index:
        start_index = folder_index
        end_index = folder_index + 1
    else:
        start_index = 0
        end_index = None

    for folder in folders[start_index:end_index]:
        # Get the annotation file from the folder
        # TODO this likely needs to be updated so just grabs one of them
        annotation_file = get_annotation_file(folder)
        # Check that the annotation file exists
        if not os.path.isfile(annotation_file):
            continue

        # Fix frame numbers in place
        if fix_frame:
            fix_frame_number(annotation_file)

        # Parse the input image names
        write_image_list(annotation_file, folder, image_list_file, is_sealion)

        output_file = (
            os.path.join(output_dir, method + "_" + os.path.basename(folder)) + ".csv"
        )
        kwcoco_file = annotation_file.replace(".viame.csv", "_watershed.kwcoco.json")
        if convert_only:
            ub.cmd(f"dvc unprotect {kwcoco_file}", verbose=3)
            convert(output_file, kwcoco_file)
            # TODO see what needs to be done here
            # ub.cmd(f"cp {output_file}.csv {updated_file}", verbose=3)
            add_cmd = f"dvc --cd {os.path.dirname(kwcoco_file)} add {os.path.basename(kwcoco_file)}"
            print(add_cmd)
            ub.cmd(add_cmd, verbose=3)
            continue

        if run:
            if debug:
                cmd_string = "gdb -ex 'run' --args " + cmd_string

            cmd_string = (
                f"kwiver runner {method}.pipe "
                + f" --setting input:video_filename='{image_list_file}' --setting "
                + f"detector_writer:file_name='{output_file}' "
                f"--setting detection_reader:file_name='{annotation_file}'"
            )
            os.chdir(pipeline_dir)
            ub.cmd(cmd_string, verbose=3)

        if check_output_counts:
            annotation_count = 0
            output_count = 0
            for line in open(annotation_file):
                annotation_count += 1
            for line in open(output_file):
                output_count += 1
            print(
                f"annotation lines {annotation_count}, outputlines {output_count}, diff {annotation_count - output_count}"
            )


if __name__ == "__main__":
    args = parse_args()
    if args.validate_lengths:
        validate_lengths(
            "/home/local/KHQ/david.russell/data/viame_dvc/public/Aerial/US_ALASKA_MML_SEALION",
            args.output_dir,
            # "/home/local/KHQ/david.russell/data/NOAA_sea_lion/watershed",
        )
        exit()

    compute(
        root_dir=args.input_dir,
        output_dir=args.output_dir,
        pipeline_dir=args.pipeline_dir,
        method=args.method,
        debug=args.debug,
        check_output_counts=args.check_output_counts,
        run=args.run,
        folder_index=args.folder_index,
        image_list_file=args.image_list_file,
        is_sealion=False,
    )
