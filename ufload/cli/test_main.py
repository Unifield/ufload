import ufload.cli.main

class Arg:    
    def __init__(self):
        self.db_prefix = 'pfx'
    
def test_file_to_db():
    x = "../foo/OCG_MM1_WA-20160831-220427-A-UF2.1-2p3.dump"
    i = ufload.cli.main._file_to_db(Arg(), x)
    assert(i == "pfx_OCG_MM1_WA_20160831_2204")
    i = ufload.cli.main._file_to_db(Arg(), "wrong.dump")
    assert(i is None)


