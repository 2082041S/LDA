import json


class ConfigData:
    def __init__(self, host, port, authkey, directory_paths, K, eta, alpha, normalise):
        self.host = host
        self.port = port
        self.authkey = authkey
        self.directory_paths = directory_paths
        self.K = K
        self.eta = eta
        self.alpha = alpha
        self.normalise = normalise


# configuration data is a dictionary with "Connection" and "LDA" as first level keys
# and second level keys being information we are interested in
# i.e config_dict[Connection][host]
def get_value_from_config_file(config_dict, key_level_1, key_level_2, default_value):
    if config_dict.has_key(key_level_1):
        if config_dict[key_level_1].has_key(key_level_2):
            if config_dict[key_level_1][key_level_2] is not None:
                desired_value = config_dict[key_level_1][key_level_2]
            else:
                desired_value = default_value
        else:
            raise ValueError("setup file is missing " + key_level_2 + " within " + key_level_1)
    else:
        raise ValueError("setup file does not have information about " + key_level_1)

    if isinstance(default_value, int):
        try:
            desired_value = int(desired_value)
        except:
            raise TypeError(key_level_2 + " must be of type int, but it is of type " +
                            str(type(desired_value)))

    if isinstance(default_value, float):
        try:
            desired_value = float(desired_value)
        except:
            raise TypeError(key_level_2 + " must be of type float, but it is of type " +
                            str(type(desired_value)))

    return desired_value

# gets only the connection config data
# used by worker client
def get_connection_data():
    with open("C:\LDA\LDA\config.json", 'r') as json_data_file:
        config_dict = json.load(json_data_file)
    print config_dict
    host = get_value_from_config_file(config_dict, "Connection", "host", "")
    port = get_value_from_config_file(config_dict, "Connection", "port", 8765)
    authkey = get_value_from_config_file(config_dict, "Connection", "authkey", "password")
    config_data = ConfigData(host, port, authkey, directory_paths="", K=0, eta=0, alpha=0, normalise=0)
    return config_data

# gets full config data
# used by master server
def get_config_data():
    with open("C:\LDA\LDA\config.json", 'r') as json_data_file:
        config_dict = json.load(json_data_file)
    print config_dict
    host = get_value_from_config_file(config_dict, "Connection", "host", "")
    port = get_value_from_config_file(config_dict, "Connection", "port", 8765)
    authkey = get_value_from_config_file(config_dict, "Connection", "authkey", "password")
    directory_paths = get_value_from_config_file(config_dict, "LDA", "input_files_directory_paths",[])
    if not directory_paths:
        raise ValueError("No directory paths provided")
    K = get_value_from_config_file(config_dict, "LDA", "K", 500)
    eta = get_value_from_config_file(config_dict, "LDA", "eta", 0.01)
    alpha = get_value_from_config_file(config_dict, "LDA", "alpha", 1)
    normalise = get_value_from_config_file(config_dict, "LDA", "normalise", 1000)
    config_data = ConfigData(host, port, authkey, directory_paths, K, eta, alpha, normalise)
    return config_data
