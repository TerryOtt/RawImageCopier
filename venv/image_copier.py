import os
import argparse
import logging
import exiftool
import json
import pprint
import datetime

logging_level = logging.DEBUG

def _parse_args():
    arg_parser = argparse.ArgumentParser(description="Copy RAW files to NAS")
    arg_parser.add_argument( "exiftool_path", help="Full path to ExifTool.exe")
    arg_parser.add_argument( "source_dir", help="Root directory of new raw files")
    arg_parser.add_argument( "image_file_extension",
                            help="file extension for RAW files, e.g. 'CR3', 'RAW'")
    arg_parser.add_argument( "file_timestamp_utc_offset_hours",
                             help="Hours offset from UTC, e.g. EDT is -4, Afghanistan is 4.5",
                             type=float)

    return arg_parser.parse_args()

def _enumerate_source_images(args, matching_file_extension):
    image_files = []
    print(
        f"Scanning \"{args.source_dir}\" for RAW image files with extension \"{matching_file_extension}\"")

    for subdir, dirs, files in os.walk(args.source_dir):
        #logging.debug(f"Found subdir {subdir}")
        for filename in files:
            if filename.lower().endswith( matching_file_extension ):
                file_absolute_path = os.path.join( args.source_dir, subdir, filename)
                #logging.debug( "Found image with full path: \"{0}\"".format(file_absolute_path))
                image_files.append( file_absolute_path )
            else:
                #logging.debug( "Skipping non-image file {0}".format(filename))
                pass

    return image_files


def _get_exif_datetimes( args, source_image_files ):

    exiftool_tag_name = "EXIF:DateTimeOriginal"

    with exiftool.ExifTool(args.exiftool_path) as exiftool_handle:

        for curr_image_file in source_image_files:
            #logging.debug(f"Getting EXIF metadata for \"{curr_image_file}\"")

            exif_datetime = exiftool_handle.get_tag(exiftool_tag_name, curr_image_file)

            #logging.debug( f"File \"{curr_image_file}\" has EXIF datetime \"{exif_datetime}\"")

            # Create legit datetime, not tz aware (yet)
            file_datetime_no_tz = datetime.datetime.strptime( exif_datetime, "%Y:%m:%d %H:%M:%S")

            #logging.debug( f"Parsed datetime: {file_datetime_no_tz.isoformat()}")

            # Do hour shift from timezone-unaware EXIF datetime to UTC
            shifted_datetime_no_tz = file_datetime_no_tz + datetime.timedelta(
                hours=-(args.file_timestamp_utc_offset_hours))

            # Create TZ-aware datetime, as one should basically alwyys use
            file_datetime_utc = shifted_datetime_no_tz.replace(tzinfo=datetime.timezone.utc)

            #logging.debug( f"{curr_image_file}: {file_datetime_utc.isoformat()}")

    print( "Done printing metadata")



def _main():
    logging.basicConfig(level=logging_level)
    args = _parse_args()
    matching_file_extension = args.image_file_extension.lower()
    if matching_file_extension.startswith( "." ) is False:
        matching_file_extension = "." + matching_file_extension

    source_image_files = _enumerate_source_images( args, matching_file_extension )

    print( "Found {0} files with extension \"{1}\" under \"{2}\"".format(
        len(source_image_files), matching_file_extension, args.source_dir) )

    # Find EXIF dates for all source image files
    files_by_date = _get_exif_datetimes( args, source_image_files )

if __name__ == "__main__":
    _main()
