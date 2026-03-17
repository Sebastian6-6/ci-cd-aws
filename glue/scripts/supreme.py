import os
from dotenv import load_dotenv

load_dotenv()
MARCA_ALVO = os.getenv("MARCA_CORRETA")

def verificacao_supreme():
    menu = (
        " Iniciando verificação de identidade...\n"
        " 1 - Adidas\n"
        " 2 - Nike\n"
        " 3 - Lacoste/\n"
        " 4 - New Balance\n"
        " 5 - Fila\n"
        " 6 - Converse\n"
        " 7 - Vans\n"
        " 8 - Supreme\n"
        "Escolha o número da Marca"
        )
    
    supreme_input = []
    
    for i in range(5):
        entrada = input(f"{menu} [{i+1}/5]:")
        
        # 1. Verificação de tamanho (mais de 1 dígito)
        if len(entrada) > 1:
            print(f"ERRO: Você digitou '{entrada}'. Use apenas um dígito!")
            return False
        
        # 2. Verificação se é número e se está no intervalo 1-8
        if not entrada.isdigit() or int(entrada) < 1 or int(entrada) > 8:
            print(f"ERRO: '{entrada}' é inválido. Escolha apenas números de 1 a 8.")
            return False
            
        supreme_input.append(int(entrada))
    
    marcas = {
         1 : "Adidas",
         2 : "Nike",
         3 : "Lacoste",
         4 : "New Balance",
         5 : "Fila",
         6 : "Converse",
         7 : "Vans",
         8 : "Supreme"
}
    
    escolhas_nomes = [marcas[n] for n in supreme_input]
    
    if escolhas_nomes == [MARCA_ALVO] * 5:
        print("\nREDIRECIONANDO....SUUUUUUUUUUUUPREEEEEEEEEEEEEEMEEEEEEEEEEEEEEEE")
        return True
    else: 
        print(f"\nVocê escolheu errado: {escolhas_nomes} \nTente outra vez!")
        return False