import ufload.cli.main

def test_file_to_db():
    x = "../foo/OCG_MM1_WA-20160831-220427-A-UF2.1-2p3.dump"
    i = ufload.cli.main._file_to_db(x)
    assert(i == "OCG_MM1_WA_20160831_2204")
    i = ufload.cli.main._file_to_db("wrong.dump")
    assert(i is None)


