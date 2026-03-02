"""Reconstruct gross LT activity from Table 1.4A netting layers.

Table 1.4A has three nesting levels of LT capital gain/loss data:

LEVEL 1 (Transaction-level): Sub-category gains/losses from direct sales
  - 4 sub-categories by reporting type (basis/no basis, 8949/no 8949)
  - Each has: sales price, cost basis, adjustment, gain, loss
  - These ARE transaction-level: SP - CB + Adj = Gain - Loss exactly

LEVEL 2 (Per-return net by source): Cols 66/68
  - Net LT gain/loss from all sales of capital assets
  - = Level 1 totals MINUS per-return within-sales netting

LEVEL 3 (Per-return overall net): Cols 62/64
  - Overall net LT gain/loss across ALL sources
  - = Level 2 + opaque sources MINUS cross-source per-return netting

This script traces each layer and estimates what gross activity might be
hidden inside the opaque partnership/S-corp and "other forms" sources.
"""

import xlrd
from src.etl.agi_bins import match_agi_bin


def val(sh, r, c):
    """Read a cell value, convert to dollars (multiply by 1000)."""
    v = sh.cell_value(r, c)
    try:
        return float(str(v).replace(',', '').replace('[', '').replace(']', '').strip() or '0') * 1000
    except (ValueError, TypeError):
        return 0


def fmt(v):
    """Format a dollar amount for display."""
    if abs(v) >= 1e9:
        return f'{v/1e9:>8.1f}B'
    elif abs(v) >= 1e6:
        return f'{v/1e6:>8.0f}M'
    else:
        return f'{v/1e3:>8.0f}K'


def main():
    wb = xlrd.open_workbook('data/raw/2022/22in14acg.xls')
    sh = wb.sheet_by_index(0)

    # Define AGI groups
    agi_groups = {
        'Under 30K':  [1, 2, 3, 4, 5, 6, 7],
        '30K-100K':   [8, 9, 10, 11],
        '100K-500K':  [12, 13],
        '500K-5M':    [14, 15, 16, 17],
        '5M+':        [18, 19],
    }

    # Read all data rows
    all_rows = {}
    for r in range(9, sh.nrows):
        label = str(sh.cell_value(r, 0)).strip()
        if not label:
            continue
        if label.startswith('[') or label.startswith('*') or 'NOTE:' in label:
            break
        bin_id = match_agi_bin(label)
        if bin_id is None or bin_id in all_rows:
            continue

        all_rows[bin_id] = {
            # Level 3: overall net
            'lt_gain_overall': val(sh, r, 62),
            'lt_loss_overall': val(sh, r, 64),
            # Level 2: sales net
            'lt_gain_sales': val(sh, r, 66),
            'lt_loss_sales': val(sh, r, 68),
            # Level 1: transaction-level sub-categories
            'sub_gain_a': val(sh, r, 74), 'sub_loss_a': val(sh, r, 76),
            'sub_gain_b': val(sh, r, 84), 'sub_loss_b': val(sh, r, 86),
            'sub_gain_c': val(sh, r, 94), 'sub_loss_c': val(sh, r, 96),
            'sub_gain_d': val(sh, r, 104), 'sub_loss_d': val(sh, r, 106),
            # Opaque sources
            'gain_other': val(sh, r, 108),
            'loss_other': val(sh, r, 110),
            'gain_ps': val(sh, r, 112),
            'loss_ps': val(sh, r, 114),
            'cap_gain_dist': val(sh, r, 116),
            'lt_carry': val(sh, r, 118),
        }

    print('=' * 110)
    print('RECONSTRUCTION OF GROSS LT ACTIVITY FROM TABLE 1.4A NETTING LAYERS')
    print('Tax Year 2022 (amounts in dollars)')
    print('=' * 110)

    summary_rows = []

    for grp_name, bin_ids in agi_groups.items():
        # Aggregate bins
        d = {}
        for key in all_rows[1].keys():
            d[key] = sum(all_rows.get(b, {}).get(key, 0) for b in bin_ids)

        # Transaction-level gross from sales sub-categories
        gross_gain_txn = d['sub_gain_a'] + d['sub_gain_b'] + d['sub_gain_c'] + d['sub_gain_d']
        gross_loss_txn = d['sub_loss_a'] + d['sub_loss_b'] + d['sub_loss_c'] + d['sub_loss_d']

        # Netting absorbed at Level 2 (within-sales per-return netting)
        netting_L2_gain = gross_gain_txn - d['lt_gain_sales']
        netting_L2_loss = gross_loss_txn - d['lt_loss_sales']

        # All source gains feeding into Level 3
        all_source_gains = d['lt_gain_sales'] + d['gain_other'] + d['gain_ps'] + d['cap_gain_dist']
        all_source_losses = d['lt_loss_sales'] + d['loss_other'] + d['loss_ps']

        # Netting absorbed at Level 3 (cross-source per-return netting)
        netting_L3_gain = all_source_gains - d['lt_gain_overall']
        netting_L3_loss = all_source_losses - d['lt_loss_overall']

        # Transaction-level loss ratio (VISIBLE SALES ONLY)
        txn_denom = gross_gain_txn + gross_loss_txn
        txn_loss_ratio = gross_loss_txn / txn_denom if txn_denom > 0 else 0

        # Observable empirical ratio (from calibration_validate.py formula)
        emp_denom = d['lt_gain_overall'] + d['lt_loss_overall'] + d['lt_carry']
        empirical_ratio = (d['lt_loss_overall'] + d['lt_carry']) / emp_denom if emp_denom > 0 else 0

        # Source composition
        total_visible = d['lt_gain_sales'] + d['gain_other'] + d['gain_ps'] + d['cap_gain_dist']

        print()
        print(f'--- {grp_name} ---')
        print()
        print(f'  LAYER 1: Transaction-Level (Direct Sales Only)')
        print(f'    Gross gains from sales:        {fmt(gross_gain_txn)}')
        print(f'    Gross losses from sales:       {fmt(gross_loss_txn)}')
        print(f'    Transaction loss ratio:         {txn_loss_ratio:.1%}')
        print()
        print(f'  LAYER 2: Per-Return Netting (Within Sales)')
        print(f'    Net LT gain from sales:        {fmt(d["lt_gain_sales"])}')
        print(f'    Net LT loss from sales:        {fmt(d["lt_loss_sales"])}')
        print(f'    Gain absorbed by netting:      {fmt(netting_L2_gain)}')
        print(f'    Loss absorbed by netting:      {fmt(netting_L2_loss)}')
        print()
        print(f'  OPAQUE SOURCES (Net K-1 only):')
        print(f'    Partnership/S-corp gain:       {fmt(d["gain_ps"])}')
        print(f'    Partnership/S-corp loss:       {fmt(d["loss_ps"])}')
        print(f'    Other forms gain:              {fmt(d["gain_other"])}')
        print(f'    Other forms loss:              {fmt(d["loss_other"])}')
        print(f'    Cap gain distributions:        {fmt(d["cap_gain_dist"])}')
        print()
        print(f'  LAYER 3: Per-Return Netting (Cross-Source)')
        print(f'    Overall LT gain (col 62):      {fmt(d["lt_gain_overall"])}')
        print(f'    Overall LT loss (col 64):      {fmt(d["lt_loss_overall"])}')
        print(f'    LT loss carryover (col 118):   {fmt(d["lt_carry"])}')
        print(f'    Gain absorbed by netting:      {fmt(netting_L3_gain)}')
        print(f'    Loss absorbed by netting:      {fmt(netting_L3_loss)}')
        print()
        print(f'  RATIOS:')
        print(f'    Transaction-level loss ratio (sales only):   {txn_loss_ratio:.1%}')
        print(f'    Empirical ratio (cols 62/64/118):             {empirical_ratio:.1%}')
        print()
        if total_visible > 0:
            print(f'  SOURCE COMPOSITION (% of visible LT gains):')
            print(f'    Direct sales:           {d["lt_gain_sales"]/total_visible:>6.1%}')
            print(f'    Partnership/S-corp:     {d["gain_ps"]/total_visible:>6.1%}')
            print(f'    Other forms:            {d["gain_other"]/total_visible:>6.1%}')
            print(f'    Cap gain distributions: {d["cap_gain_dist"]/total_visible:>6.1%}')

        summary_rows.append({
            'group': grp_name,
            'gross_gain_txn': gross_gain_txn,
            'gross_loss_txn': gross_loss_txn,
            'txn_loss_ratio': txn_loss_ratio,
            'netting_L2': netting_L2_gain,
            'gain_ps': d['gain_ps'],
            'loss_ps': d['loss_ps'],
            'gain_other': d['gain_other'],
            'loss_other': d['loss_other'],
            'netting_L3': netting_L3_gain,
            'empirical_ratio': empirical_ratio,
            'pct_sales': d['lt_gain_sales'] / total_visible if total_visible else 0,
            'pct_ps': d['gain_ps'] / total_visible if total_visible else 0,
        })

    # RECONSTRUCTION under different assumptions
    print()
    print('=' * 110)
    print('RECONSTRUCTION: Estimated Gross LT Loss Ratios Under Different')
    print('Assumptions About Partnership/S-Corp Internal Loss Harvesting')
    print('=' * 110)
    print()
    print('The key question: What fraction of partnership/S-corp and other-forms')
    print('activity is INVISIBLE because it was netted internally before the K-1?')
    print()
    print('Assumption: "Internal loss ratio" = fraction of GROSS partnership activity')
    print('that consists of harvested losses (netted away before K-1 reporting).')
    print()
    print('If a partnership has $100 gross gains and $40 gross losses internally,')
    print('the K-1 reports only $60 net gain. The internal loss ratio = 40%.')
    print()

    # For each assumed internal loss ratio, reconstruct
    assumed_ratios = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60]

    header = f'{"AGI Group":<12} {"Txn LR":>7} {"Emp LR":>7}'
    for ar in assumed_ratios:
        header += f' {"LR@"+str(int(ar*100))+"%":>7}'
    header += f'  {"Model":>7}'
    print(header)
    print('-' * len(header))

    model_ratios = {
        'Under 30K': 0.70,
        '30K-100K': 0.75,
        '100K-500K': 0.80,
        '500K-5M': 0.85,
        '5M+': 0.90,
    }

    for sr in summary_rows:
        line = f'{sr["group"]:<12} {sr["txn_loss_ratio"]:>6.1%} {sr["empirical_ratio"]:>6.1%}'

        for ar in assumed_ratios:
            # Reconstruct: for opaque sources, if internal loss ratio = ar,
            # then reported_net_gain = gross_gain * (1 - ar)
            # so gross_gain = reported_net_gain / (1 - ar)
            # and gross_loss = gross_gain * ar / (1 - ar) * reported_net_gain = ar/(1-ar) * net
            # Wait, more carefully:
            # gross_gain_ps = net_gain_ps / (1 - ar)  (if net is positive)
            # gross_loss_ps = gross_gain_ps * ar / (1 - ar)
            # Actually: if net_gain = gross_gain - gross_loss, and ar = gross_loss/gross_gain
            # then gross_gain = net_gain / (1 - ar)
            # and gross_loss = net_gain * ar / (1 - ar)

            if ar >= 1.0:
                line += f'    N/A'
                continue

            # Reconstruct gross for partnerships
            ps_gross_gain = sr['gain_ps'] / (1 - ar) if sr['gain_ps'] > 0 else sr['gain_ps']
            ps_gross_loss = sr['gain_ps'] * ar / (1 - ar) if sr['gain_ps'] > 0 else 0

            # Same for other forms
            other_gross_gain = sr['gain_other'] / (1 - ar) if sr['gain_other'] > 0 else sr['gain_other']
            other_gross_loss = sr['gain_other'] * ar / (1 - ar) if sr['gain_other'] > 0 else 0

            # Also account for visible losses from P/S and other (these are on losing returns)
            ps_gross_loss += abs(sr['loss_ps'])
            other_gross_loss += abs(sr['loss_other'])

            # Total reconstructed gross
            total_gross_gain = sr['gross_gain_txn'] + ps_gross_gain + other_gross_gain
            total_gross_loss = sr['gross_loss_txn'] + ps_gross_loss + other_gross_loss
            # Add netting layers and carryover as additional loss evidence
            # Actually, netting absorbed = real losses that offset gains on same return
            # This is already captured in the per-return netting

            denom = total_gross_gain + total_gross_loss
            reconstructed_ratio = total_gross_loss / denom if denom > 0 else 0
            line += f' {reconstructed_ratio:>6.1%}'

        line += f'  {model_ratios[sr["group"]]:>6.0%}'
        print(line)

    print()
    print('Legend:')
    print('  Txn LR  = Transaction-level loss ratio (visible direct sales only)')
    print('  Emp LR  = Empirical ratio from cols 62/64/118 (per-return overall net)')
    print('  LR@XX%  = Reconstructed loss ratio assuming XX% internal loss ratio')
    print('            for partnership/S-corp and other-forms sources')
    print('  Model   = Current model assumption (reform_assumptions.json)')
    print()
    print('NOTE: This reconstruction estimates what the OVERALL loss ratio would be')
    print('if partnerships internally harvested losses at the assumed rate before')
    print('reporting net amounts on K-1s. The "true" internal rate is unknown but')
    print('likely varies by income (higher-income = more sophisticated = higher rate).')


if __name__ == '__main__':
    main()
