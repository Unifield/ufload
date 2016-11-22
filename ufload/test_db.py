import ufload

class ArgNone:
    def __init__(self):
        self.db_prefix = None

class ArgPfx:
    def __init__(self):
        self.db_prefix = "prod"

def test_db_to_inst():
    i = ufload.db._db_to_instance(ArgNone(), "OCG_KG1_OSH_20161116_0102")
    assert i == "OCG_KG1_OSH"
    i = ufload.db._db_to_instance(ArgPfx(), "prod_OCG_KG1_OSH_20161116_0102")
    assert i == "OCG_KG1_OSH"
    i = ufload.db._db_to_instance(ArgPfx(), "prod_OCBZW160_20161116_0102")
    assert i == "OCBZW160"
    i = ufload.db._db_to_instance(ArgPfx(), "prod_BD_DHK_OCA_20161116_0102")
    assert i == "BD_DHK_OCA"
    i = ufload.db._db_to_instance(ArgPfx(), "prod_HQ_OCA_20161116_0102")
    assert i == "HQ_OCA"

