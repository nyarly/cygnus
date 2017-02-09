package swaggering

import (
	"fmt"
	"os"
	"path/filepath"
)

func InferPackage(dirName string) (packageName string, err error) {
	goPath, ok := os.LookupEnv("GOPATH")
	if !ok {
		err = fmt.Errorf("Cannot infer a package name without $GOPATH")
		return
	}

	packageName, err = packageUnderGopath(goPath, dirName)

	return
}

func packageUnderGopath(goPath, dirName string) (packageName string, err error) {
	dirName, err = filepath.Abs(dirName)
	if err != nil {
		return
	}
	dirName = filepath.Clean(dirName)

	goPath = filepath.Join(goPath, "src")

	packageName, err = filepath.Rel(goPath, dirName)
	if err != nil {
		return
	}

	if packageName[0] == '.' {
		err = fmt.Errorf("Cannot infer package name: target directory is outside of $GOPATH (%q)", goPath)
	}

	return
}
