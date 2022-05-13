import os


def get_environmental_variable(variable):
    """ A function which helps us to get environmental variables and raises an error if a key do not exist """

    env_variable = os.getenv(variable, None)

    if env_variable is None:
        print("ERROR: MISSING {}".format(variable))

        exit(1)

    return env_variable
