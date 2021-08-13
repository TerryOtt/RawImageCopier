import os
import argparse
import logging
import exiftool
import json
import pprint
import datetime
import time

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

    start_time = time.perf_counter()

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

    end_time = time.perf_counter()

    operation_time_seconds = end_time - start_time

    logging.debug( f"Enumerate time: {(operation_time_seconds):.03f} seconds" )

    return {
        'image_files'               : image_files,
        'operation_time_seconds'    : operation_time_seconds,
    }


def _get_exif_datetimes( args, source_image_files ):

    exiftool_tag_name = "EXIF:DateTimeOriginal"

    file_data = {}

    file_count = len(source_image_files)

    print( f"Getting EXIF timestamps for {file_count} files")

    col_width = len(str(file_count))

    display_increment_file_count = 25

    start_time = time.perf_counter()

    with exiftool.ExifTool(args.exiftool_path) as exiftool_handle:

        for (file_index, curr_image_file) in enumerate(source_image_files):
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
            file_data[ curr_image_file ] = {
                'datetime'  : file_datetime_utc
            }

            display_file_index = file_index + 1

            if (display_file_index % display_increment_file_count == 0) or (display_file_index == file_count):
                percent = int((display_file_index / file_count) * 100)
                print( f"\tTimestamps: {display_file_index:>{col_width}d} / {file_count:>{col_width}d} ({percent:>3d}%)")

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    print( f"Completed obtaining timestamps ({operation_time_seconds:.03f} seconds)" )

    return {
        'file_data'                 : file_data,
        'operation_time_seconds'    : operation_time_seconds,
    }


def _add_perf_timing(perf_timings, label, value):
    perf_timings['entries'].append(
        {
            'label' : label,
            'value' : value,
        }
    )
    perf_timings['total'] += value


def _display_perf_timings( perf_timings ):
    # Find longest label
    longest_label_len = len('Total')
    for entry in perf_timings['entries']:
        if len(entry['label']) > longest_label_len:
            longest_label_len = len(entry['label'])

    print( "\nPerformance data:\n" )

    for curr_entry in perf_timings['entries']:
        percentage_time = (curr_entry['value'] / perf_timings['total']) * 100.0
        print( f"\t{curr_entry['label']:>{longest_label_len}s} : {curr_entry['value']:>7.03f} seconds " +
               f"({percentage_time:5.01f}%)")

    total_label = "Total"
    print (f"\n\t{total_label:>{longest_label_len}s} : {perf_timings['total']:>7.03f} seconds" )


def _main():
    logging.basicConfig(level=logging_level)
    args = _parse_args()
    matching_file_extension = args.image_file_extension.lower()
    if matching_file_extension.startswith( "." ) is False:
        matching_file_extension = "." + matching_file_extension

    perf_timings = {
        'total' : 0.0,
        'entries': [],
    }

    enumerate_output = _enumerate_source_images( args, matching_file_extension )
    _add_perf_timing( perf_timings, 'File scan', enumerate_output['operation_time_seconds'] )

    print( "Found {0} files with extension \"{1}\" under \"{2}\" ({3:7.06f} seconds)".format(
        len(enumerate_output['image_files']), matching_file_extension, args.source_dir,
        enumerate_output['operation_time_seconds']))

    # Find EXIF dates for all source image files
    datetime_scan_output = _get_exif_datetimes( args, enumerate_output['image_files'] )
    _add_perf_timing( perf_timings, 'EXIF Timestamps', datetime_scan_output['operation_time_seconds'])

    _display_perf_timings( perf_timings )

if __name__ == "__main__":
    _main()
