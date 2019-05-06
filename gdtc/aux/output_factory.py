
def generate_db_output_getter_template(obj):
    output = {}
    output["db_host"] = obj.params["output_db_host"]
    output["db_port"] = obj.params["output_db_port"]
    output["db_database"] = obj.params["output_db_database"]
    output["db_user"] = obj.params["output_db_user"]
    output["db_password"] = obj.params["output_db_password"]
    output["db_table"] = obj.params["output_db_table"]

    return output

def generate_db_output_setter_template(obj, output):
    obj.params["output_db_host"] = output["db_host"]
    obj.params["output_db_port"] = output["db_port"]
    obj.params["output_db_database"] = output["db_database"]
    obj.params["output_db_user"] = output["db_user"]
    obj.params["output_db_password"] = output["db_password"]
    obj.params["output_db_table"] = output["db_table"]

    return obj