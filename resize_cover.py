#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
resize_cover.py
~~~~~~~~~~~~

Creates PNG images at 100x130 and a 160x200 of a journal cover image file passed as the first argument.
 The generated images maintain the original aspect ratio, and use a white background by default.  To use
 a different background color, specify it as the second argument.

 Requires pillow (up-to-date version of PIL)

The files are placed into the appropriate subdirectories of dryad-repo, so change DRYAD_REPO_ROOT if you're not me.

"""
import sys
try:
    from PIL import Image, ImageColor
except ImportError as e:
    sys.stderr.write('\nERROR: Pillow Not found. This script uses Pillow to read/write image files.\nSee http://pillow.readthedocs.org/en/latest/index.html for installation instructions\n\n')
    sys.exit(1)
from os.path import basename, splitext, normpath, expanduser
import argparse

__author__ = 'dan.leehr@nescent.org'


FRONT_COVER_DIMS = (100, 130)
PKG_COVER_DIMS = (160, 200)

DRYAD_REPO_ROOT =  "/Users/dan/Code/dryad-repo"

class CoverGenerator(object):
    def __init__(self, repo_root, filename):
        self.repo_root = repo_root
        self.filename = filename
        self.base = splitext(basename(filename))[0]
        self.image = None
        self.resized = None
        self.dims = None
        self.output_filename = None
        self.bgcolor = "rgba(255,255,255,255)"

    def front_output_dir(self):
        return normpath(self.repo_root + "/dspace/modules/xmlui/src/main/webapp/themes/Mirage/images/")

    def pkg_output_dir(self):
        return normpath(self.repo_root + "/dspace/modules/xmlui/src/main/webapp/themes/Dryad/images/coverimages/")

    def read_image(self):
        if self.image is None:
            self.image = Image.open(self.filename)
        return

    def generate_front_cover(self):
        """ Generate a cover image sized for the dryad homepage """
        self.read_image()
        self.dims = FRONT_COVER_DIMS
        return self.generate_cover()

    def generate_pkg_cover(self):
        """ Generate a cover image sized for the data package page """
        self.dims = PKG_COVER_DIMS
        return self.generate_cover()

    def calculate_aspect_preserved_size(self):
        widthratio = float(self.dims[0]) / float(self.image.size[0])
        heightratio = float(self.dims[1]) / float(self.image.size[1])

        ratio = min(widthratio, heightratio)
        self.aspect_preserved_size = (int(self.image.size[0] * ratio), int(self.image.size[1] * ratio))


    def generate_cover(self):
        """ Generate a cover image with the specified size """
        self.calculate_aspect_preserved_size()
        resized = self.image.copy().resize(self.aspect_preserved_size,Image.ANTIALIAS)
        if resized.mode != 'RGB' and resized.mode != 'RGBA':
            resized = resized.convert("RGB")
        # paste the resized into a new image
        color = ImageColor.getrgb(self.bgcolor)
        frame = Image.new(resized.mode, self.dims, color=color)
        box = ((self.dims[0] - resized.size[0])/2, (self.dims[1] - resized.size[1])/2)
        if resized.mode == 'RGBA':
            frame.paste(resized, box, resized)
        else:
            frame.paste(resized, box)
        self.resized = frame

    def write_cover(self):
        self.resized.save(self.output_filename, "PNG")

    def write_front_cover(self):
        self.generate_front_cover()
        self.output_filename = self.front_output_dir() + "/recentlyIntegrated-" + self.base + ".png"
        self.write_cover()

    def write_pkg_cover(self):
        self.generate_pkg_cover()
        self.output_filename = self.pkg_output_dir() + "/" + self.base + ".png"
        self.write_cover()

class DefaultHelpParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

def generate_covers(repo_root, filename, color=None):
    generator = CoverGenerator(expanduser(repo_root), expanduser(filename))
    if color is not None:
        generator.bgcolor = color
    generator.write_front_cover()
    generator.write_pkg_cover()

def main():
    parser = DefaultHelpParser(
        description='Resize journal cover images and place in your local Dryad codebase',
        epilog='Images can be in any format supported by pillow (JPEG/PNG/GIF/etc). Images will be sized to fit 100x130 and 160x200 and placed into the appropriate subdirectories within a Dryad git working copy.\nImage aspect ratios will be preserved, and borders will be filled with white (unless other color is specified)',
        add_help=True,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('repo_root', metavar='/path/to/dryad-repo', type=str)
    parser.add_argument('filename', metavar='image-filename.png', type=str)
    parser.add_argument('--color', '-c',  metavar='"rgba(\'255,255,255,255)\'',type=str)
    args = vars(parser.parse_args())
    generate_covers(args['repo_root'], args['filename'], args['color'])

if __name__ == '__main__':
    main()
