if __name__ == "__main__":
    my_map = BasicMap(-105.2705, 40.015, 0.5, 0.25)
    my_map.describe()

    try:
        print("Calculating bounds...")
        my_map.get_bounds()
    except TypeError:
        print("Error: in get_bounds - Check your input values, they must be numbers!")