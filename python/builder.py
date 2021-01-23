from shutil import copyfile
from os import makedirs
from pathlib import Path

from traversedir import search_dir_for_sas
from sas_parsers import simple_sas_2_rst
from toc import Root
from srcpath import SrcPath
from srcpath import ReSTPath


class Builder:


    __ROOT_STEM = 'index'


    def __init__(self, dir_path):
        self._srcs = [ SrcPath(p) for p in search_dir_for_sas(dir_path) ]
        self._dir_path  = dir_path
        

    def write_reST_files(self, dst_dir):

        for src in self._srcs:
            # check if we actually have a SAS file? Eh..
            
            with open(str(src.sas_path())) as fp:
                sas_src_str = fp.read()

            macro_lib = simple_sas_2_rst(sas_src_str)
            macro_lib.add_src_path(src.sas_path().resolve())
            reST_str = macro_lib.make_reST()
            
            out_path = src.reST_root_change(self._dir_path, dst_dir)
            makedirs(str(out_path.parent), exist_ok=True)
            print('Writing to ' + str(out_path))
            with open(str(out_path), 'w') as out_fp:
                out_fp.write(reST_str)
            
        # Build the root reST file
        docnames_for_toc = [ s.docname(self._dir_path) for s in self._srcs ]
        root_str = Root(docnames_for_toc).make_reST()

        # Write root reST file
        root_path = dst_dir / (self.__ROOT_STEM + ReSTPath.REST_EXT)
        print('Writing to ' + str(root_path))
        with open(str(root_path), 'w') as root_fp:            
            root_fp.write(root_str)


    # src: ReSTPath
    def _dst_path_str(self, src, dst_dir):
        ret = src.reST_path().relative_to(self._dir_path)
        return str(dst_dir / ret)