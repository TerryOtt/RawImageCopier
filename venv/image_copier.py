import os
import argparse
import logging

logging_level = logging.DEBUG

def _parse_args():
    arg_parser = argparse.ArgumentParser(description="Copy RAW files to NAS")
    arg_parser.add_argument( "source_dir", help="Root directory of new raw files")
    arg_parser.add_argument( "image_file_extension",
                            help="file extension for RAW files, e.g. 'CR3', 'RAW'")
    return arg_parser.parse_args()

def _enumerate_source_images(args):
    logging.info(
        f"Scanning \"{args.source_dir}\" for image files with extension {args.image_file_extension}")
    image_files = []
    for subdir, dirs, files in os.walk(args.source_dir):
        #logging.debug(f"Found subdir {subdir}")
        for filename in files:
            if filename.lower().endswith( args.image_file_extension.lower() ):
                file_absolute_path = os.path.join( args.source_dir, subdir, filename)
                #logging.debug( "Found image with full path: \"{0}\"".format(file_absolute_path))
                image_files.append( file_absolute_path )
            else:
                #logging.debug( "Skipping non-image file {0}".format(filename))
                pass

    return image_files

def _main():
    logging.basicConfig(level=logging_level)
    args = _parse_args()
    source_image_files = _enumerate_source_images( args )

    print( "Found {0} files with extension {1} under \"{2}\"".format(
        len(source_image_files), args.image_file_extension, args.source_dir) )

if __name__ == "__main__":
    _main()
