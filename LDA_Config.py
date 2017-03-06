import json


class ConfigData:
    def __init__(self, host, port, authkey, directory_paths, e_step_its, K, eta, alpha, normalise, 
                    max_vocabulary_length, document_appearance_min_threshold):
        self.host = host
        self.port = port
        self.authkey = authkey
        self.directory_paths = directory_paths
        self.e_step_its = e_step_its
        self.K = K
        self.eta = eta
        self.alpha = alpha
        self.normalise = normalise
        self.max_vocabulary_length = max_vocabulary_length
        self.document_appearance_min_threshold = document_appearance_min_threshold


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

# gets only the clients config data
def get_client_data():
    with open("config.json", 'r') as json_data_file:
        config_dict = json.load(json_data_file)
    print config_dict
    host = get_value_from_config_file(config_dict, "Connection", "host", "")
    port = get_value_from_config_file(config_dict, "Connection", "port", 8765)
    authkey = get_value_from_config_file(config_dict, "Connection", "authkey", "password")
    e_step_its = get_value_from_config_file(config_dict, "LDA", "e_step_its", 1)
    config_data = ConfigData(host, port, authkey, "", e_step_its, K=0, eta=0, alpha=0, normalise=0,
                             max_vocabulary_length = 0, document_appearance_min_threshold = 0)
    return config_data

# gets only the servera config data
def get_server_data():
    with open("config.json", 'r') as json_data_file:
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
    max_vocabulary_length = get_value_from_config_file(config_dict, "Master", "max_vocabulary_length", 50000)
    document_appearance_min_threshold = get_value_from_config_file(config_dict, "Master", "document_appearance_min_threshold", 5)
    e_step_its = 0
    config_data = ConfigData(host, port, authkey, directory_paths, e_step_its, K, eta, alpha, normalise,
                            max_vocabulary_length, document_appearance_min_threshold)
    return config_data
