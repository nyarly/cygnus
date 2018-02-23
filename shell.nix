with import <nixpkgs> {};

buildGoPackage rec {
  name = "cygnus";
  buildInputs = [];
  goPackagePath = "github.com/nyarly/cygnus";
}
