from configparser import ConfigParser

def load_config(filename='helper/database.ini', section='postgresql'):
    """
    Reads a configuration file and returns the parameters for a specific section.

    Parameters:
    filename (str): The name of the configuration file to read. Default is 'database.ini'.
    section (str): The section in the configuration file to retrieve parameters from. Default is 'postgresql'.

    Returns:
    dict: A dictionary containing the parameters for the specified section.

    Raises:
    Exception: If the specified section is not found in the configuration file.
    """
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        # print(params)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in {1} file'.format(section,filename))
    
    return config

if __name__ == '__main__':
    config = load_config()
    # print(config)