import re

class ValidaCPF:
    def __init__(self, cpf: str):
        self.cpf = re.sub(r'\D', '', cpf)  # Remove qualquer caractere que não seja dígito

    def validar_cpf(self) -> bool:
        if len(self.cpf) != 11:
            return False

        numeros = [int(digito) for digito in self.cpf]

        # Verifica se todos os dígitos são iguais
        if len(set(numeros)) == 1:
            return False

        # Validação do primeiro dígito verificador
        soma_produtos = sum(a * b for a, b in zip(numeros[0:9], range(10, 1, -1)))
        digito_esperado = (soma_produtos * 10 % 11) % 10
        if numeros[9] != digito_esperado:
            return False

        # Validação do segundo dígito verificador
        soma_produtos1 = sum(a * b for a, b in zip(numeros[0:10], range(11, 1, -1)))
        digito_esperado1 = (soma_produtos1 * 10 % 11) % 10
        if numeros[10] != digito_esperado1:
            return False

        return True