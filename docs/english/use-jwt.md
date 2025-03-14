# Use JWT

## Motivation

If you have many users and it's not feasible to maintain entries in a password file for them, use JWT.
Access rights can be embedded in JWT. JWT can be generated dynamically.

# Configuration

Create a JWT secrets file named jwt.yaml:
```yaml
default:  # name of secret
    alg: HS512
    secret: '*****'
```

Add the JWT secrets file configuration to your config:
```yaml
...
auth:
  enable: true
  password_file: /path/to/passwords/file
  jwt:
    # The file with secrets
    secrets_file: jwt.yaml
...
```

# Secret Rotation

```yaml
default:  # name of secret
    alg: HS512
    secret: '*****'
my_old_deprecated_secret:  # drop after 2035-03-12
    alg: HS256
    secret: '*****'
```

The example shows two secrets for rotating deprecated secrets. Initially, start with one secret.
The secret named my_old_deprecated_secret is our old secret. It has been compromised and needs to be phased out.
For a smooth phase-out, you need to:

1. add a new secret alongside the old one
2. monitor the akasa_jwt metric
3. once the metric with the secret name label drops below the threshold
4. remove the old secret from the file
