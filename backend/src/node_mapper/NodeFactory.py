from importlib import import_module as load


def load_node_type(node_name, init_params: dict = {}):
    """
    Dynamic import the node
    """
    module = load(f'node_mapper.{node_name}')
    cls = getattr(module, node_name)
    instance = cls(init_params)
    return instance


class NodeFactory():
    """
    Node generator factory
    """

    @staticmethod
    def new_node(name, params: dict = None):
        """
        Generates a new node
        """
        return load_node_type(name, params)