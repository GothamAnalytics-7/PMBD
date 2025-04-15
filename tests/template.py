import os


def criar_pasta(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Criada pasta: {path}")
    else:
        print(f"Pasta já existe: {path}")


def criar_estrutura_projeto():
    pastas = [
        f"data/raw",
        f"data/processed",
        f"data/external",
        f"notebooks",
        f"src/data",
        f"src/features",
        f"src/models",
        f"src/visualization",
        f"src/utils",
        f"docs/images",
        f"tests",
    ]

    for pasta in pastas:
        criar_pasta(pasta)

    # Criar ficheiro __init__.py no diretório src para marcar como módulo Python
    init_file = os.path.join(f"src", "__init__.py")
    with open(init_file, 'w') as f:
        pass
    print(f"Criado ficheiro: {init_file}")


if __name__ == "__main__":
    criar_estrutura_projeto()
