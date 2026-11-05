"""Developer template: implement Source methods; never emit raw exception text."""
class Source:
    version='replace-with-your-version'
    def spec(self): raise NotImplementedError
    def check(self, config): raise NotImplementedError
    def discover(self, config): raise NotImplementedError
    def read(self, config, stream, state): raise NotImplementedError
