from frictionless import Plugin, Step


# Plugin


class StylePlugin(Plugin):
    code = "style"
    status = "experimental"

    def create_step(self, descriptor):
        if descriptor.get("code") == "style":
            return style_step(descriptor)


# Pipeline Step


class style_step(Step):
    """Style a resource"""

    code = "style"

    def __init__(self, descriptor=None):
        super().__init__(descriptor)

    def transform_resource(self, resource):
        pass
