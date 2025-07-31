
class ext2int:
    def __init__(self):
        self.mapping = {}
    
    def remap_id(self, i):
        if i not in self.mapping:
            self.mapping[i] = len(self.mapping)
        return self.mapping[i]