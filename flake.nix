{
  description = "Jazz development environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # General development
            git
            turbo
            
            # JS development
            nodejs_22
            pnpm_9

            # C++ development
            clang_20
            clang-tools
            cmake
            pkg-config
          ];

          shellHook = ''
            echo ""
            echo "Welcome to the Jazz development environment!"
            echo "Run 'pnpm install' to install dependencies."
            echo ""
          '';
        };
      });
}
