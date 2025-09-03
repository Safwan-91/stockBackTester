
def BB_lb_signal_check(row):
    if row['close'] < row['lower_bb']:
        return True
    return False


def BB_ub_signal_check(row):
    if row['close'] > row['upper_bb']:
        return True
    return False


def rsi_oversold_signal_check(row):
    if row['rsi'] < 30:
        return True
    return False


def rsi_overbought_signal_check(row):
    if row['rsi'] > 70:
        return True
    return False

