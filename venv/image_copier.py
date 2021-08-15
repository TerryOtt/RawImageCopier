import os
import argparse
import logging
import queue
import exiftool
import json
import pprint
import datetime
import time
import glob
import multiprocessing
import time
import shutil
import pathlib
import gpxpy


logging_level = logging.DEBUG
num_worker_processes = int(multiprocessing.cpu_count() * 1.0)

def _parse_args():
    arg_parser = argparse.ArgumentParser(description="Copy RAW files to NAS")
    arg_parser.add_argument( "exiftool_path", help="Full path to ExifTool.exe")
    arg_parser.add_argument( "source_dir", help="Root directory of new raw files")
    arg_parser.add_argument( "image_file_extension",
                            help="file extension for RAW files, e.g. 'CR3', 'RAW'")
    arg_parser.add_argument( "file_timestamp_utc_offset_hours",
                             help="Hours offset from UTC, e.g. EDT is -4, Afghanistan is 4.5",
                             type=float)
    arg_parser.add_argument( "gpx_file_path", help="Path to GPX file for geocoding these pictures")
    arg_parser.add_argument( "destination_root", help="Root of destination directory (e.g., \"Q:\Lightroom\Images\")")

    return arg_parser.parse_args()


def _enumerate_source_images(args, matching_file_extension):
    image_files = []
    print(
        f"\nScanning \"{args.source_dir}\" for RAW image files with extension \"{matching_file_extension}\"")

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

    time_start = time.perf_counter()

    file_data = {}
    file_count = len(source_image_files)

    print( f"\nStarting EXIF timestamp extraction for {file_count} files")

    start_time = time.perf_counter()

    process_handles = []

    #  Queue for sending files needing timestamps to children
    files_to_process_queue = multiprocessing.Queue(maxsize=file_count)

    # Queue that children use to write EXIF timestamp information data back to parent
    processed_file_queue = multiprocessing.Queue(maxsize=file_count)

    for i in range(num_worker_processes):
        process_handle = multiprocessing.Process( target=_exif_timestamp_worker,
                                                  args=(i+1, files_to_process_queue,
                                                        processed_file_queue,
                                                        args) )

        process_handle.start()
        #logging.debug(f"Parent back from start on child process {i+1}")
        process_handles.append( process_handle )

    # Load up the queue with all the files to process
    for curr_file_info in source_image_files:
        #logging.debug(f"About to write {json.dumps(curr_file_info)} to the child queue")
        files_to_process_queue.put(curr_file_info)

    # We know how many files we wrote into the queue that children read out of. Now read
    #   same number of entries out of the processed data queue
    for i in range( file_count ):
        exif_timestamp_data = processed_file_queue.get()
        file_data[ exif_timestamp_data['file_path']] = exif_timestamp_data

    #logging.debug( f"Parent process has read out all {file_count} entries from results queue" )

    # Rejoin child threads
    while process_handles:
        curr_handle = process_handles.pop()
        #logging.debug("parent process waiting for child worker to rejoin")
        curr_handle.join()
        #logging.debug("child process has rejoined cleanly")

    #logging.debug("Parent process exiting, all EXIF timestamp work done")

    time_end = time.perf_counter()

    operation_time_seconds = time_end - time_start

    print( f"Completed EXIF timestamps extraction")

    return {
        'file_data'                 : file_data,
        'operation_time_seconds'    : operation_time_seconds,
    }


def _exif_timestamp_worker( child_process_index, files_to_process_queue, processed_file_queue, args ):
    #print( f"Child {child_process_index} started")

    exiftool_tag_name = "EXIF:DateTimeOriginal"

    with exiftool.ExifTool(args.exiftool_path) as exiftool_handle:

        while True:
            try:
                # No need to wait, the queue was pre-loaded by the parent
                curr_file_entry = files_to_process_queue.get( timeout=0.1 )
            except queue.Empty:
                # no more work to be done
                #print( f"Child {child_process_index} found queue empty on get, bailing from processing loop")
                break

            # print(
            #     f"Child {child_process_index} read processing entry from queue: {json.dumps(curr_file_entry, indent=4, sort_keys=True)}")

            absolute_path = curr_file_entry['file_path']

            exif_datetime = exiftool_handle.get_tag(exiftool_tag_name, absolute_path)

            # Create legit datetime object from string, note: not tz aware (yet)
            file_datetime_no_tz = datetime.datetime.strptime(exif_datetime, "%Y:%m:%d %H:%M:%S")

            # Do hour shift from timezone-unaware EXIF datetime to UTC
            shifted_datetime_no_tz = file_datetime_no_tz + datetime.timedelta(
                hours=-(args.file_timestamp_utc_offset_hours))

            # Create TZ-aware datetime, as one should basically always strive to use
            file_datetime_utc = shifted_datetime_no_tz.replace(tzinfo=datetime.timezone.utc)

            file_data = {
                'file_path'         : absolute_path,
                'filesize_bytes'    : curr_file_entry['filesize_bytes'],
                'datetime'          : file_datetime_utc,
            }

            processed_file_queue.put( file_data )

    #print( f"Child {child_process_index} exiting cleanly")



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


def _set_unique_destination_filename( source_file, file_data, args, matching_file_extension, filename_conflict_dict,
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
            glob_match_str = os.path.join( year_date_subfolder, f"*{matching_file_extension}" )
            #logging.debug( f"Glob match string: {glob_match_str}")

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
        # Need to come up with a non-conflicting name
        if index_append is None:
            index_append = 1
        else:
            index_append += 1

        next_attempt_name = os.path.join( year_date_subfolder,
                                          basename_minus_ext + f"-{index_append:04d}" + file_extension )

        logging.info( f"Found destination filename conflict with \"{test_file_path}\", trying \"{next_attempt_name}\"" )

        test_file_path = next_attempt_name

    #logging.debug( f"Found unique destination filename: {test_file_path}")
    # Mark the final location for this file
    file_data[ 'unique_destination_file_path' ] = test_file_path

    # Add final location to our conflict list
    filename_conflict_dict[ test_file_path ] = None

    #logging.debug( f"Updated file info:\n{json.dumps(file_data, indent=4, sort_keys=True, default=str)}")


def _set_destination_filenames( args, matching_file_extension, file_data ):

    start_time = time.perf_counter()

    sorted_files = sorted(file_data.keys())

    destination_filename_conflict_dict = {}
    destination_dirs_scanned = {}

    for curr_file in sorted_files:
        #logging.debug(f"Getting size and unique destination filename for {curr_file}")
        #logging.debug(f"\tSize: {file_size} bytes")

        _set_unique_destination_filename( curr_file, file_data[curr_file],
            args, matching_file_extension, destination_filename_conflict_dict,
            destination_dirs_scanned )

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return operation_time_seconds


def _geocode_images( args, file_data ):
    print( "\n\tReading GPX file" )
    with open( args.gpx_file_path, "r" ) as gpx_file_handle:
        gpx_data = gpxpy.parse( gpx_file_handle )

    print("\tGPX file parsing complete")

    print("\n\tGeocoding all images")
    for curr_file_name in file_data:
        curr_file_data = file_data[curr_file_name]
        computed_location = gpx_data.get_location_at( curr_file_data['datetime'] )
        if computed_location:
            #logging.debug( f"Computed location: {computed_location}")
            filedata_location = {
                'latitude_wgs84_degrees'              : computed_location[0].latitude,
                'longitude_wgs84_degrees'             : computed_location[0].longitude,
                'elevation_above_sea_level'     : {
                    'meters'    : computed_location[0].elevation,
                    'feet'      : computed_location[0].elevation * 3.28084,
                },
            }

            curr_file_data['geocoded_location'] = filedata_location

            logging.debug( f"Set file's geolocated location to:\n{json.dumps(filedata_location, indent=4, sort_keys=True)}")
        else:
            print(f"WARN: Could not geocode file \"{curr_file_name}\"")
    print("\tAll images geocoded" )

def _do_file_copies( args, file_data ):
    time_start = time.perf_counter()

    file_count = len( file_data.keys() )

    #print( f"Copying {file_count} image files to \"{args.destination_root}\"" )

    #  Queue for sending information to worker processes about files needing to be copied
    files_to_copy_queue = multiprocessing.JoinableQueue(maxsize=file_count)

    process_handles = []

    # Play with number of copy processes
    copy_worker_count = 1

    for i in range(copy_worker_count):

        process_handle = multiprocessing.Process(target=_file_copy_worker,
                                                 args=(i + 1, files_to_copy_queue,
                                                       args))

        process_handle.start()
        # logging.debug(f"Parent back from start on child process {i+1}")
        process_handles.append(process_handle)

    # Load up the queue with all the files to copy
    for curr_file_name in file_data:
        # logging.debug(f"About to write {json.dumps(curr_file_info)} to the child queue")
        files_to_copy_queue.put(file_data[curr_file_name] )

    # Wait for children to finish copy, which will be signaled when last entry is marked done
    files_to_copy_queue.join()

    # Rejoin all child threads
    while process_handles:
        curr_handle = process_handles.pop()
        curr_handle.join()

    logging.debug( "All copy workers have rejoined cleanly" )

    time_end = time.perf_counter()
    operation_time_seconds = time_end - time_start

    return operation_time_seconds


def _file_copy_worker( worker_index, files_to_copy_queue, args ):

    while True:
        try:
            # No need to wait, the queue was pre-loaded by the parent
            curr_file_entry = files_to_copy_queue.get(timeout=0.1)
        except queue.Empty:
            # no more work to be done
            #print( f"Worker {worker_index} queue empty on get, bailing from processing loop")
            break

        #print( json.dumps( curr_file_entry, indent=4, sort_keys=True, default=str) )

        curr_source_file = curr_file_entry['file_path']

        #print( f"Worker {worker_index} doing copy for {curr_source_file}" )

        # Do we need to make either of the subfolders (YYYY/YYYY-MM-DD)?
        curr_folder = curr_file_entry['destination_subfolders']['date']
        try:
            pathlib.Path( curr_folder ).mkdir( parents=True, exist_ok=True )
        except:
            print(f"Exception thrown in creating dirs for {curr_folder}")

        # Attempt copy
        try:
            dest_path = curr_file_entry['unique_destination_file_path']
            shutil.copyfile(curr_source_file, dest_path)
            #print( f"\"{curr_source_file}\" -> \"{dest_path}\" - OK OK OK")

        except:
            print(f"Exception thrown when copying {curr_file_entry['file_path']}" )

        # Mark task done or parent will deadlock
        files_to_copy_queue.task_done()

    #print( f"Worker {worker_index} exiting cleanly")


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
    _add_perf_timing( perf_timings, 'Scanning for RAW Files', enumerate_output['operation_time_seconds'] )

    #logging.debug( f"Cumulative bytes: {enumerate_output['cumulative_bytes']}")

    print(
        "Found {0} files with extension \"{1}\" under \"{2}\", totalling {3:.01f} MB / {4:.01f} GB ({5:7.06f} seconds)".format(
        len(enumerate_output['image_files']), matching_file_extension, args.source_dir,
        enumerate_output['cumulative_bytes'] / (1024 * 1024),
        enumerate_output['cumulative_bytes'] / (1024 * 1024 * 1024),
        enumerate_output['operation_time_seconds']))

    # Find EXIF dates for all source image files
    datetime_scan_output = _get_exif_datetimes( args, enumerate_output['image_files'] )
    file_data = datetime_scan_output['file_data']
    _add_perf_timing( perf_timings, 'Obtaining EXIF Timestamps', datetime_scan_output['operation_time_seconds'])

    # Geocode all source image files based on their timestamp, the user-provided UTC hour offset, and the
    #   user-provided GPX file
    print( "\nStarting geocoding" )
    geocoding_operation_time_seconds = _geocode_images( args, file_data )
    print( "Geocoding complete")

    # Determine unique filenames
    print( "\nGetting unique filenames in destination folder")
    set_destination_filenames_operation_time = _set_destination_filenames( args, matching_file_extension, file_data )
    print( "Unique file names in destination folder are set")
    _add_perf_timing( perf_timings, 'Generating Unique Destination Filenames', set_destination_filenames_operation_time )

    # We can now (finally!) perform all file copies
    print( "\nStarting file copies")
    copy_operation_time_seconds = _do_file_copies( args, file_data )
    print( "File copies completed")
    _add_perf_timing( perf_timings, 'Copying Files to Destination', copy_operation_time_seconds )


    # Final perf print
    _display_perf_timings( perf_timings )

    # Display TODOs to jog my brain
    print( "\n\n******************************************************************************************\n"
           "TODO: read in GPX file for this photo set, use the UTC offset passed in, geotag the photo,\n"
           "      write to XMP sidecar file that Bridge/Lightroom/Capture One can use\n" 
           "******************************************************************************************" )


if __name__ == "__main__":
    _main()