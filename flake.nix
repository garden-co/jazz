{
  description = "Jazz development environment";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-25.05";
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
