import os
import os
import argparse
import logging
import exiftool
import json
import pprint
import datetime
import time
import glob

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
    arg_parser.add_argument( "destination_root", help="Root of destination directory (e.g., \"Q:\Lightroom\Images\")")

    return arg_parser.parse_args()


def _enumerate_source_images(args, matching_file_extension):
    image_files = []
    print(
        f"Scanning \"{args.source_dir}\" for RAW image files with extension \"{matching_file_extension}\"")

    start_time = time.perf_counter()
    cumulative_bytes = 0

    for subdir, dirs, files in os.walk(args.source_dir):
        #logging.debug(f"Found subdir {subdir}")
        for filename in files:
            if filename.lower().endswith( matching_file_extension ):
                file_absolute_path = os.path.join( args.source_dir, subdir, filename)
                #logging.debug( "Found image with full path: \"{0}\"".format(file_absolute_path))
                file_size_bytes = os.path.getsize(file_absolute_path)
                #logging.debug(f"File size of {file_absolute_path}: {file_size_bytes}")
                cumulative_bytes += file_size_bytes
                image_files.append(
                    {
                        'file_path'         : file_absolute_path,
                        'filesize_bytes'    : file_size_bytes,
                    }
                )
                #break
            else:
                #logging.debug( "Skipping non-image file {0}".format(filename))
                pass

        #if len(image_files) > 0:
            #break

    end_time = time.perf_counter()

    operation_time_seconds = end_time - start_time

    #logging.debug( f"Enumerate time: {(operation_time_seconds):.03f} seconds" )

    return {
        'image_files'               : image_files,
        'cumulative_bytes'          : cumulative_bytes,
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

            absolute_path = curr_image_file['file_path']

            exif_datetime = exiftool_handle.get_tag(exiftool_tag_name, absolute_path)

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
            file_data[ absolute_path ] = {
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


def _set_unique_destination_filename( source_file, file_data, args, filename_conflict_dict,
                                      destination_dirs_scanned ):
    #logging.debug( f"Setting destination filename for {source_file} under {args.destination_root}")

    # Folder structure is YYYY\YY-MM-DD\[unique filename]

    date_components = {
        'year'          : file_data['datetime'].year,
        'date_iso8601'  : \
            f"{file_data['datetime'].year:4d}-{file_data['datetime'].month:02d}-{file_data['datetime'].day:02d}",
    }

    # merge the date_component data into the file_data
    file_data.update( date_components )

    year_subfolder = os.path.join( args.destination_root, str(date_components['year']))
    year_date_subfolder = os.path.join( year_subfolder, date_components['date_iso8601'] )

    file_data['destination_subfolders'] = {
        'year': year_subfolder,
        'date': year_date_subfolder,
    }

    # Have we added existing files in this subfolder to the conflict_dict already?
    if not year_date_subfolder in destination_dirs_scanned:
        if os.path.isdir( year_subfolder) is True and os.path.isdir( year_date_subfolder ) is True:
            # TODO: Enumerate all matching files in the directory
            glob_match_str = f"{year_date_subfolder}/*{args.image_file_extension}"
            logging.debug( f"Glob match string: {glob_match}")

            files_matching_glob = glob.glob( glob_match_str )

            # add any files in this dir
            for curr_match in files_matching_glob:
                filename_conflict_dict[curr_match] = None

            #logging.debug( f"Added {len(files_matching_glob)} files from \"{year_date_subfolder}\" to conflict list")
        else:
            #logging.debug( f"Subfolder \"{year_date_subfolder}\" does not exist, added to list of dirs we have scanned" )
            pass

        # Regardless of which logic path we took, we can now say we've scanned that directory
        destination_dirs_scanned[ year_date_subfolder ] = None
    else:
        #logging.debug( f"Already scanned directory \"{year_date_subfolder}\", skipping")
        pass

    # Find first filename that doesn't exist in conflict list
    basename = os.path.basename( source_file )
    (basename_minus_ext, file_extension) = os.path.splitext(basename)

    test_file_path = os.path.join( year_date_subfolder, basename )
    index_append = None

    while test_file_path in filename_conflict_dict:
        logging.debug ( f"\"{test_file_path}\" is a conflict")
        # Need to come up with a non-conflicting name
        if index_append is None:
            index_append = 1
        else:
            index_append += 1

        test_file_path = os.path.join( year_date_subfolder, basename_minus_ext + f"-{index_append:04d}" +
                                       file_extension )

        #logging.debug( f"Found conflict, trying new name {test_file_path}")



    #logging.debug( f"Found unique destination filename: {test_file_path}")
    # Mark the final location for this file
    file_data[ 'unique_destination_file_path' ] = test_file_path

    # Add final location to our conflict list
    filename_conflict_dict[ test_file_path ] = None

    #logging.debug( f"Updated file info:\n{json.dumps(file_data, indent=4, sort_keys=True, default=str)}")


def _set_destination_filenames( args, file_data ):

    start_time = time.perf_counter()

    sorted_files = sorted(file_data.keys())

    destination_filename_conflict_dict = {}
    destination_dirs_scanned = {}

    for curr_file in sorted_files:
        #logging.debug(f"Getting size and unique destination filename for {curr_file}")
        #logging.debug(f"\tSize: {file_size} bytes")

        _set_unique_destination_filename( curr_file, file_data[curr_file],
            args, destination_filename_conflict_dict,
            destination_dirs_scanned )


    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return {
        'operation_time_seconds'        : operation_time_seconds,
    }



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
    _add_perf_timing( perf_timings, 'File Scan', enumerate_output['operation_time_seconds'] )

    #logging.debug( f"Cumulative bytes: {enumerate_output['cumulative_bytes']}")

    print(
        "Found {0} files with extension \"{1}\" under \"{2}\", totalling {3:.01f} MB / {4:.01f} GB ({5:7.06f} seconds)".format(
        len(enumerate_output['image_files']), matching_file_extension, args.source_dir,
        enumerate_output['cumulative_bytes'] / (1024 * 1024),
        enumerate_output['cumulative_bytes'] / (1024 * 1024 * 1024),
        enumerate_output['operation_time_seconds']))

    # Find EXIF dates for all source image files
    datetime_scan_output = _get_exif_datetimes( args, enumerate_output['image_files'] )
    _add_perf_timing( perf_timings, 'EXIF Timestamps', datetime_scan_output['operation_time_seconds'])

    # Determine unique filenames
    file_data = datetime_scan_output['file_data']
    final_metadata = _set_destination_filenames( args, file_data )
    _add_perf_timing( perf_timings, 'Unique Destination Filenames', final_metadata['operation_time_seconds'])



    # Final perf print
    _display_perf_timings( perf_timings )


if __name__ == "__main__":
    _main()