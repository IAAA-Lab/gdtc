# Module for classes and functions to manipulate coordinate systems and their definitions

class Srs():
    """
    Encapsulates SRS/CRS definitions in the different formats used by the different underlying geolibraries
    """
    def __init__(self, epsg_string):
        """
        :param epsg_string: in the format 'EPSG:4326' for instance
        """
        self.epsg_string = epsg_string

    def as_epsg_number(self):
        return self.epsg_string.split(':')[1]

