class BulkUpload:
    def __init__(self,options) -> None:
        if isinstance(options,dict):
            self.options = options
        else:
            raise Exception("options must be a dict")
    def run(self):

        pass
