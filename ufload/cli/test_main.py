import ufload.cli.main

def test_find_instance():
    x = "../foo/OCG_MM1_WA-20160831-220427-A-UF2.1-2p3.dump"
    i = ufload.cli.main._find_instance(x)
    assert(i == "OCG_MM1_WA_20160831_2204")
    i = ufload.cli.main._find_instance("wrong.dump")
    assert(i is None)


