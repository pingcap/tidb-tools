# sqlgen

sqlgen is a tool for generating a random generator. 

After the execution, a golang package named *pkgname* is generated, which contains a set of files. The `Generate()` locating in *production*.go can be used to generate a random string, according to the syntax described in *bnf*.

## How to use

```
Usage of sqlgen:
  -bnf string
        A list of bnf files
  -destination string
        The destination of generated package located (default ".")
  -pkgname string
        The name of package to be generated
  -production string
        The production to be generated

```

