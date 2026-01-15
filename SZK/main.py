import Astana
import Erastov
import Fomin
import Hunters
import Koodrenko
import Kvitsiniya
import Other
import Saljenikina
import Unnamed
import Urtenova
import Vse_scheta

def run_module(mod):
    try:
        mod.main()
    except Exception as e:
        print(f"Ошибка в модуле {mod.__name__}: {e}")

def main():
    modules = [
        Astana, Erastov, Fomin, Hunters, Koodrenko, Kvitsiniya,
        Other, Saljenikina, Unnamed, Urtenova, Vse_scheta
    ]
    for mod in modules:
        run_module(mod)

if __name__ == "__main__":
    main()
