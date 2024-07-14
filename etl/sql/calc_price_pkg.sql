PACKAGE BODY     ipa.calc_price_pkg
IS
    PROCEDURE update_next_day_price_full(p_date DATE, p_source VARCHAR2) AS
    BEGIN
        update_next_day_price_index(p_date, p_source);
        insert_estimated_adjust(p_date, p_source);
        update_next_day_price_stock(p_date, p_source);
        update_next_day_price_warrant(p_date, p_source);
        update_delisting_security(p_date, p_source);
    END;

    PROCEDURE process_adjust_stock_by_code(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) AS

    BEGIN
        ipa.write_log('calc_price_pkg','process_adjust_stock_by_code '||p_code,SYSDATE, NULL, NULL, 'ADJUST_STOCK', 'START');
            --Cap nhat ty le dieu chinh dung tu SKQ
            insert_final_adjust_ratio(p_code,p_date,p_source);
            --Dieu chinh gia ifo_sec_price theo ma
            adjust_sec_price_by_code(p_code);
        ipa.write_log('calc_price_pkg','process_adjust_stock_by_code '||p_code,SYSDATE,SYSDATE, NULL, 'ADJUST_SEC_PRICE', 'SUCCESS');
            --Dieu chinh gia ifo_sec_intra_history va ifo_trading_summary theo ma
            adjust_intra_price_by_code(p_code);
        ipa.write_log('calc_price_pkg','process_adjust_stock_by_code '||p_code,SYSDATE,SYSDATE, NULL, 'ADJUST_INTRA_PRICE', 'SUCCESS');
        ipa.write_log('calc_price_pkg','process_adjust_stock_by_code '||p_code,SYSDATE,SYSDATE, NULL, 'ADJUST_STOCK', 'SUCCESS');

    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','process_adjust_stock_by_code '||p_code,NULL,SYSDATE,SUBSTR(SQLERRM, 1, 1000), 'ADJUST_STOCK', 'FAIL');
    END;

    PROCEDURE update_future_full(p_date DATE) AS
    BEGIN
        update_settlement_price_future(p_date);
        update_next_day_price_future(p_date);
    END;

    PROCEDURE process_adjust_stock_full(p_date DATE, p_source VARCHAR2) AS
        CURSOR c_code IS
            SELECT sec_code,adjust_date, adjust_ratio, DECODE(estimated_ratio,NULL,1,estimated_ratio) AS estimated_ratio,
                   DECODE(estimated_ratio,NULL,1,estimated_ratio)-adjust_ratio AS ratio_diff
            FROM ifo_sec_price_adjust_log
            WHERE adjust_date = ipa.get_next_working_date(p_date,1);
    BEGIN
        ipa.write_log('calc_price_pkg','process_adjust_stock_daily',SYSDATE, NULL, NULL, 'ADJUST_STOCK', 'START');
        FOR r IN c_code LOOP
            --Cap nhat ty le dieu chinh dung
            insert_final_adjust_ratio(r.sec_code,p_date,p_source);
            --Dieu chinh gia ifo_sec_price theo ma
            adjust_sec_price_by_code(r.sec_code);
        END LOOP;
        ipa.write_log('calc_price_pkg','process_adjust_stock_daily',SYSDATE,SYSDATE, NULL, 'ADJUST_SEC_PRICE', 'SUCCESS');
        -- khong tan dung process_adjust_stock_by_code ma tach intraday dieu chinh sau de BA khong phai doi lau va chuyen sang dau viec ke tiep
        FOR r IN c_code LOOP
            --Dieu chinh gia ifo_sec_intra_history va ifo_trading_summary theo ma
            adjust_intra_price_by_code(r.sec_code);
        END LOOP;
        ipa.write_log('calc_price_pkg','process_adjust_stock_daily',SYSDATE,SYSDATE, NULL, 'ADJUST_INTRA_PRICE', 'SUCCESS');
        ipa.write_log('calc_price_pkg','process_adjust_stock_daily',SYSDATE,SYSDATE, NULL, 'ADJUST_STOCK', 'SUCCESS');

    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','process_adjust_stock_daily',NULL,SYSDATE,SUBSTR(SQLERRM, 1, 1000), 'ADJUST_STOCK', 'FAIL');
    END;

    FUNCTION get_tradestatus(p_code VARCHAR2, p_date DATE) RETURN VARCHAR2 IS
        CURSOR c_newlist IS
            SELECT ipo.effective_date, si.nm_trading_qtty, NVL(si.krx_trade_status,'NWE') krx_trade_status
            FROM ifo_company_ipo_status ipo, quote.security_info si, ifo_sec_code sc
            WHERE ipo.company_id = sc.company_id
            AND sc.sec_code = si.code(+)
            AND ipo.ipo_status_type IN ('IPO3.6','IPO4.5','IPO5.5')
            AND sc.sec_code = p_code
            AND ipo.effective_date <= p_date
            AND NVL(ipo.expiration_date,'31-Dec-9999') >= p_date
            UNION ALL
            SELECT dc.first_trading_date AS effective_date, di.nm_trading_qtty, NVL(di.krx_trade_status,'NWN') krx_trade_status
            FROM ifo_derivative_code dc, quote.security_info di
            WHERE dc.derivative_code = di.code (+)
            AND dc.derivative_type = 'CW'
            AND dc.derivative_code = p_code;

        CURSOR c_trade_hist IS
            SELECT sec_code, exchange_code, trans_date, hist_volume
            FROM ipa.latest_trade_hist
            WHERE sec_code = p_code;

        v_value             VARCHAR2(3);
        v_newlist           c_newlist%rowtype;
        v_trade_hist        c_trade_hist%rowtype;
    BEGIN
        v_value := 'NRM';
        OPEN c_newlist;
        FETCH c_newlist INTO v_newlist;
            IF c_newlist%FOUND THEN
                IF v_newlist.effective_date = p_date THEN
                    v_value := 'NWE';
                    RETURN v_value;
                ELSIF v_newlist.nm_trading_qtty > 0 THEN
                    v_value := 'NRM';
                    RETURN v_value;
                --sau khi trien khai KRX va bang quote co data thi mo rao
                /*
                ELSIF v_newlist.nm_trading_qtty = 0 AND v_newlist.krx_trade_status <> 'NRM' THEN
                    v_value := v_newlist.krx_trade_status;
                    RETURN v_value;
                */
                ELSE
                    OPEN c_trade_hist;
                    FETCH c_trade_hist INTO v_trade_hist;
                        IF v_trade_hist.hist_volume < 100 AND v_trade_hist.exchange_code = 'UPCOM' THEN
                            v_value := 'SNE';
                        ELSIF v_trade_hist.hist_volume < 100 AND v_trade_hist.exchange_code <> 'UPCOM' THEN
                            v_value := 'SLS';
                        END IF;
                    CLOSE c_trade_hist;
                    RETURN v_value;
                END IF;
            END IF;
        CLOSE c_newlist;
        RETURN v_value;
    END;

    FUNCTION get_santionstatus(p_code VARCHAR2, p_date DATE) RETURN VARCHAR2 IS
        CURSOR c_santion IS
            SELECT sc.exchange_code, ipo.*
            FROM ifo_sec_code sc, ifo_company_ipo_status ipo
            WHERE sc.company_id = ipo.company_id
            AND sc.sec_code = p_code
            AND ipo.ipo_status_type IN ('IPO6.1','IPO6.3')
            AND ipo.effective_date <= p_date
            AND NVL(ipo.expiration_date,'31-Dec-9999') >= p_date
            ORDER BY ipo.effective_date DESC;

        v_value             VARCHAR2(3);
        v_santion           c_santion%rowtype;
    BEGIN
        v_value := 'NRM';
        OPEN c_santion;
        FETCH c_santion INTO v_santion;
            IF c_santion%FOUND THEN
                IF v_santion.ipo_status_type = 'IPO6.3' THEN
                    v_value := 'SUS';
                ELSIF v_santion.ipo_status_type = 'IPO6.1' AND v_santion.exchange_code = 'HOSTC' THEN
                    --v_value := 'NRM';--áp dụng cho KRX, do các mã này được giao dịch PCA
                    v_value := 'TFR'; -- dùng tạm trước khi triển KRX
                ELSIF v_santion.ipo_status_type = 'IPO6.1' AND v_santion.exchange_code <> 'HOSTC' AND to_char(p_date,'D') = 6 THEN
                    v_value := 'NRM';
                ELSIF v_santion.ipo_status_type = 'IPO6.1' AND v_santion.exchange_code <> 'HOSTC' AND to_char(p_date,'D') <> 6 THEN
                    v_value := 'TFR';
                ELSE
                    v_value := 'NRM';
                END IF;
            END IF;
        CLOSE c_santion;
        RETURN v_value;
    END;

    FUNCTION get_current_price(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) RETURN NUMBER AS
        CURSOR c_quotedata IS
             SELECT
                CASE
                    WHEN floor_code = '03'
                        THEN ROUND(ROUND(DECODE(average_price,0,basic_price,average_price),0),-2)
                    ELSE
                        DECODE(close_price,0,basic_price,close_price)
                END AS next_day_basic
             FROM quote.security_info
             WHERE code = p_code;

        CURSOR c_ipadata IS
            SELECT exchange_code
            , DECODE(exchange_code,'UPCOM',ROUND(average_price,1),'HNX',ROUND(close_price,1),'HOSTC',ROUND(close_price,3))*1000 AS price
            FROM ifo_sec_price
            WHERE sec_code = p_code
            AND trans_date = p_date
            UNION ALL
            SELECT exchange_code, DECODE(stock_type,'FU',NVL(settlement_price,close_price),close_price) AS price
            FROM ifo_derivative_trade
            WHERE derivative_code = p_code
            AND trans_date = p_date;

        CURSOR c_newlist IS
            SELECT base_price
            FROM ifo_company_ipo_status
            WHERE ipo_status_type IN ('IPO3.6','IPO4.5','IPO5.5')
            AND company_id = ipa.getcompanyid(p_code)
            AND effective_date = ipa.get_next_working_date(p_date,1)
            UNION ALL
            SELECT
                --dc.derivative_code      AS sec_code,
                --dc.exchange_code,
                round(dc.issue_price * pt.close_price / pi.close_price * substr(si.exercise_ratio, 1, length(si.exercise_ratio) - 2) /
                substr(st.exercise_ratio, 1, length(st.exercise_ratio) - 2), - 1) / 1000 AS base_price
                --, dc.underlying_asset     AS underlying,
                --dc.last_trading_date,
                --st.exercise_ratio
            FROM
                ifo_derivative_code           dc,
                ifo_derivative_strike_price   si,
                ifo_derivative_strike_price   st,
                ifo_sec_price                 pi,
                ifo_sec_price                 pt
            WHERE dc.derivative_code = p_code
                AND dc.first_trading_date = ipa.get_next_working_date(p_date,1)
                AND dc.derivative_type = 'CW'
                AND dc.derivative_code = si.derivative_code
                AND dc.derivative_code = st.derivative_code
                AND dc.underlying_asset = pi.sec_code
                AND dc.underlying_asset = pt.sec_code
                AND si.effective_date = (
                    SELECT
                        MIN(effective_date)
                    FROM
                        ifo_derivative_strike_price
                    WHERE
                        derivative_code = si.derivative_code
                )
                AND st.effective_date <= dc.first_trading_date
                AND nvl(st.expiry_date, '31-Dec-9999') >= dc.first_trading_date
                AND pi.trans_date = (
                    SELECT
                        MAX(trans_date)
                    FROM
                        ifo_sec_price
                    WHERE
                        sec_code = dc.underlying_asset
                        AND trans_date < si.effective_date
                )
                AND pt.trans_date = (
                    SELECT
                        MAX(trans_date)
                    FROM
                        ifo_sec_price
                    WHERE
                        sec_code = dc.underlying_asset
                        AND trans_date < dc.first_trading_date
                );

        v_result            NUMBER;
        v_quotedata         c_quotedata%rowtype;
        v_ipadata           c_ipadata%rowtype;
        v_newlist           c_newlist%rowtype;

    BEGIN
        IF p_source = 'QUOTE' THEN
            OPEN c_quotedata;
            FETCH c_quotedata INTO v_quotedata;
            IF c_quotedata%FOUND THEN
                v_result := ROUND(v_quotedata.next_day_basic,0);
            ELSE
                OPEN c_newlist;
                FETCH c_newlist INTO v_newlist;
                IF c_newlist%FOUND THEN
                    v_result := v_newlist.base_price;
                END IF;
                CLOSE c_newlist;
            END IF;
            CLOSE c_quotedata;
        ELSIF p_source = 'IPA' THEN
            OPEN c_ipadata;
            FETCH c_ipadata INTO v_ipadata;
            IF c_ipadata%FOUND THEN
                v_result := v_ipadata.price;
            ELSE
                OPEN c_newlist;
                FETCH c_newlist INTO v_newlist;
                IF c_newlist%FOUND THEN
                    v_result := v_newlist.base_price;
                END IF;
                CLOSE c_newlist;
            END IF;
            CLOSE c_ipadata;
        END IF;
        RETURN v_result;
    END;

    FUNCTION get_next_day_raw_basic(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) RETURN NUMBER AS
        CURSOR c_rights IS
            SELECT a.*
                , 0 AS treasury_flag --khi nao investor_rights co flag danh dau quyen dung treasury thi sua
            FROM ipa.t_adjust_price(p_code,p_date,p_source) a;

        v_result            NUMBER;
        v_unadjust_price    NUMBER;
        v_rights            c_rights%rowtype;

    BEGIN
        v_unadjust_price := get_current_price(p_code, p_date, p_source);
        --check huong quyen
        OPEN c_rights;
        FETCH c_rights INTO v_rights;
        IF c_rights%FOUND THEN
            v_result := v_rights.adjust_price;
        ELSE
            v_result := v_unadjust_price;
        END IF;
        CLOSE c_rights;

        RETURN v_result;
    END;

    FUNCTION get_next_day_round_basic(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) RETURN NUMBER AS
        CURSOR c_data IS
            SELECT ipa.get_krxmarketid(p_code,ipa.get_krxsectype(sec_type),exchange_code) AS market_id
            , ipa.get_krxsectype(sec_type) AS sec_type
            , get_next_day_raw_basic(p_code,p_date,p_source) AS raw_basic_price
            FROM ifo_sec_code
            WHERE sec_code = p_code;

        v_basic_price   NUMBER;
        v_data          c_data%rowtype;
    BEGIN
        OPEN c_data;
        FETCH c_data INTO v_data;
        IF c_data%FOUND THEN
            IF v_data.market_id = 'STO' THEN
                IF v_data.sec_type IN ('EF') THEN
                    v_basic_price := ROUND(v_data.raw_basic_price/10,0)*10; ---de dong nhat voi cach lam tron buoc gia 50 dong cho de doc
                ELSIF v_data.raw_basic_price < 10000 THEN
                    v_basic_price := ROUND(v_data.raw_basic_price/10,0)*10;
                ELSIF v_data.raw_basic_price >= 10000 AND v_data.raw_basic_price < 50000 THEN
                    v_basic_price := ROUND(v_data.raw_basic_price/50,0)*50;
                ELSE v_basic_price := ROUND(v_data.raw_basic_price/100,0)*100;
                END IF;
            ELSIF v_data.market_id = 'STX' THEN
                IF v_data.sec_type IN ('EF','MF') THEN
                    v_basic_price := ROUND(v_data.raw_basic_price,0);
                ELSE v_basic_price := ROUND(v_data.raw_basic_price,-2);
                END IF;
            ELSIF v_data.market_id = 'UPX' THEN
                v_basic_price := ROUND(v_data.raw_basic_price,-2);
            END IF;
        END IF;
        CLOSE c_data;

        RETURN v_basic_price;
    END;

    FUNCTION get_next_day_ceiling(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) RETURN NUMBER AS
        CURSOR c_data IS
            SELECT DISTINCT sc.sec_code
            , sc.sec_type
            , sc.exchange_code
            , get_tradestatus(sc.sec_code,ipa.get_next_working_date(p_date,1)) krx_trade_status
            , get_current_price(sc.sec_code,p_date,p_source) AS current_price
            , get_next_day_raw_basic(sc.sec_code,p_date,p_source) AS basic
            , si.total_volume_traded
            , DECODE(si.floor_code,'10',si.ceiling_price*10,si.ceiling_price) AS ceiling_price
            , NVL(SUM(DECODE(r.rights_type,'DIVIDEND',dividend_per_share,0)),0) AS dividend
            , NVL(SUM(r.dividend_yield),0) AS rights_flag
            , 0 AS treasury_flag
            --, NVL(SUM(DECODE(r.treasury_flag,'Y',1,0)),0) AS treasury_flag
            FROM ifo_sec_code sc
            LEFT JOIN quote.sec_info si
                ON sc.sec_code = si.code
            LEFT JOIN ifo_sec_investor_rights r
                ON sc.sec_id = r.sec_id
                AND r.rights_date = ipa.get_next_working_date(p_date,1)
                AND r.rights_type IN ('DIVIDEND','STOCKDIV','KINDDIV')
                AND NVL(r.status,'ACTIVE') NOT IN ('CANCEL','DELAY')
            WHERE sc.sec_code = p_code
            GROUP BY sc.sec_code, sc.sec_type, sc.exchange_code, si.total_volume_traded, DECODE(si.floor_code,'10',si.ceiling_price*10,si.ceiling_price);

        v_basic_price   NUMBER;
        v_result        NUMBER;
        v_market_id     VARCHAR2(3);
        v_sec_type      VARCHAR2(2);
        v_limit_extend  VARCHAR2(1);
        v_data          c_data%rowtype;

    BEGIN
        OPEN c_data;
        FETCH c_data INTO v_data;
        IF c_data%FOUND THEN
            v_basic_price := v_data.basic;
            v_sec_type := ipa.get_krxsectype(v_data.sec_type);
            v_market_id := ipa.get_krxmarketid(p_code,v_data.sec_type,v_data.exchange_code);

            IF v_data.krx_trade_status = 'NWE' OR v_data.krx_trade_status = 'SNE' THEN --KRX ap dung them SLE
                v_limit_extend := 'Y';
            ELSIF v_data.dividend > 0 AND v_data.current_price = v_basic_price THEN
                v_limit_extend := 'Y';
            ELSIF v_data.treasury_flag > 0 THEN
                v_limit_extend := 'Y';
            ELSE
                v_limit_extend := 'N';
            END IF;

            IF v_market_id = 'STO' THEN
                IF v_limit_extend = 'N' THEN
                    v_result := v_basic_price * 1.07;
                ELSE
                    v_result := v_basic_price * 1.2;
                END IF;
                IF v_sec_type IN ('EF') THEN
                    v_result := FLOOR(v_result/10)*10;
                ELSIF v_result < 10000 THEN
                    v_result := FLOOR(v_result/10)*10;
                ELSIF v_result >= 10000 AND v_result < 50000 THEN
                    v_result := FLOOR(v_result/50)*50;
                ELSE v_result := FLOOR(v_result/100)*100;
                END IF;
                IF v_result = v_basic_price THEN
                    v_result := v_basic_price + 10;
                END IF;
            ELSIF v_market_id = 'STX' THEN
                v_basic_price := ROUND(v_basic_price,-2);
                IF v_limit_extend = 'N' THEN
                    v_result := v_basic_price * 1.1;
                ELSE
                    v_result := v_basic_price * 1.3;
                END IF;
                IF v_sec_type IN ('EF','MF') THEN
                    v_result := FLOOR(v_result);
                ELSE v_result := FLOOR(v_result/100)*100;
                END IF;
                IF v_result = v_basic_price THEN
                    v_result := v_basic_price + 100;
                END IF;
            ELSIF v_market_id = 'UPX' THEN
                v_basic_price := ROUND(v_basic_price,-2);
                IF v_data.total_volume_traded = 0 AND v_data.rights_flag = 0 AND (v_data.krx_trade_status = 'SNE' OR v_data.krx_trade_status = 'NWE') THEN
                    v_result := v_data.ceiling_price;
                ELSIF v_limit_extend = 'N' THEN
                    v_result := FLOOR(v_basic_price * 1.15 / 100) * 100;
                ELSE
                    v_result := FLOOR(v_basic_price * 1.4 / 100) * 100;
                END IF;

                IF v_result = v_basic_price THEN
                    v_result := v_basic_price + 100;
                END IF;
            END IF;
            RETURN ROUND(v_result,0);
        ELSE
            v_result := 0;
        END IF;
        CLOSE c_data;
    END;

    FUNCTION get_next_day_floor_price(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) RETURN NUMBER AS
        CURSOR c_data IS
            SELECT DISTINCT sc.sec_code
            , sc.sec_type
            , sc.exchange_code
            , get_tradestatus(sc.sec_code,ipa.get_next_working_date(p_date,1)) krx_trade_status
            , get_current_price(sc.sec_code,p_date,p_source) AS current_price
            , get_next_day_raw_basic(sc.sec_code,p_date,p_source) AS basic
            , si.total_volume_traded
            , DECODE(si.floor_code,'10',si.floor_price*10,si.floor_price) AS floor_price
            , NVL(SUM(DECODE(r.rights_type,'DIVIDEND',dividend_per_share,0)),0) AS dividend
            , NVL(SUM(r.dividend_yield),0) AS rights_flag
            , 0 AS treasury_flag
            --, NVL(SUM(DECODE(r.treasury_flag,'Y',1,0)),0) AS treasury_flag
            FROM ifo_sec_code sc
            LEFT JOIN quote.sec_info si
                ON sc.sec_code = si.code
            LEFT JOIN ifo_sec_investor_rights r
                ON sc.sec_id = r.sec_id
                AND r.rights_date = ipa.get_next_working_date(p_date,1)
                AND r.rights_type IN ('DIVIDEND','STOCKDIV','KINDDIV')
                AND NVL(r.status,'ACTIVE') NOT IN ('CANCEL','DELAY')
            WHERE sc.sec_code = p_code
            GROUP BY sc.sec_code, sc.sec_type, sc.exchange_code, si.total_volume_traded, si.floor_code, si.floor_price;

        v_basic_price   NUMBER;
        v_result        NUMBER;
        v_market_id     VARCHAR2(3);
        v_sec_type      VARCHAR2(2);
        v_limit_extend  VARCHAR2(1);
        v_data          c_data%rowtype;
    BEGIN
        OPEN c_data;
        FETCH c_data INTO v_data;
        IF c_data%FOUND THEN
            v_basic_price := v_data.basic;
            v_sec_type := ipa.get_krxsectype(v_data.sec_type);
            v_market_id := ipa.get_krxmarketid(p_code,v_data.sec_type,v_data.exchange_code);

            IF v_data.krx_trade_status = 'NWE' OR v_data.krx_trade_status = 'SNE' THEN--KRX ap dung them SLE
                v_limit_extend := 'Y';
            ELSIF v_data.dividend > 0 AND v_data.current_price = v_basic_price THEN
                v_limit_extend := 'Y';
            ELSIF v_data.treasury_flag > 0 THEN
                v_limit_extend := 'Y';
            ELSE
                v_limit_extend := 'N';
            END IF;
            IF v_market_id = 'STO' THEN
                IF v_limit_extend = 'N' THEN
                    v_result := v_basic_price * (1-0.07);
                ELSE
                    v_result := v_basic_price * (1-0.2);
                END IF;
                IF v_sec_type IN ('EF') THEN
                    v_result := CEIL(v_result/10)*10;
                ELSIF v_result < 10000 THEN
                    v_result := CEIL(v_result/10)*10;
                ELSIF v_result >= 10000 AND v_result < 50000 THEN
                    v_result := CEIL(v_result/50)*50;
                ELSE v_result := CEIL(v_result/100)*100;
                END IF;
                IF v_result = v_basic_price AND v_basic_price >= 20 THEN
                    v_result := v_basic_price - 10;
                END IF;
            ELSIF v_market_id = 'STX' THEN
                IF v_limit_extend = 'N' THEN
                    v_result := v_basic_price * (1-0.1);
                ELSE
                    v_result := v_basic_price * (1-0.3);
                END IF;
                IF v_sec_type IN ('EF','MF') THEN
                    v_result := CEIL(v_result);
                ELSE v_result := CEIL(v_result/100)*100;
                END IF;
                IF v_result = v_basic_price AND v_basic_price > 200 THEN
                    v_result := v_basic_price - 100;
                END IF;
            ELSIF v_market_id = 'UPX' THEN
                IF v_data.total_volume_traded = 0 AND v_data.rights_flag = 0 AND (v_data.krx_trade_status = 'SNE' OR v_data.krx_trade_status = 'NWE') THEN
                    v_result := v_data.floor_price;
                ELSIF v_limit_extend = 'N' THEN
                    v_result := CEIL(v_basic_price * (1-0.15) / 100) * 100;
                ELSE
                    v_result := CEIL(v_basic_price * (1-0.4) / 100) * 100;
                END IF;

                IF v_result = v_basic_price AND v_basic_price >= 200 THEN
                    v_result := v_basic_price - 100;
                END IF;

            END IF;
            RETURN ROUND(v_result,0);
        ELSE
            v_result := 0;
        END IF;
        CLOSE c_data;
    END;

    PROCEDURE update_next_day_price_index(p_date DATE, p_source VARCHAR2) AS
    BEGIN
        ipa.write_log('calc_price_pkg','update_next_day_price_index '||p_date,SYSDATE,NULL,NULL, NULL, 'START');
        DELETE ipa.ifo_next_day_price WHERE UPPER(sec_type) = 'INDEX';
        COMMIT;
        IF p_source = 'QUOTE' THEN
            FOR r IN (
                SELECT mi.floor_code AS index_code, i.index_type, i.exchange_code, mi.market_index AS basic_price
                FROM quote.market_info mi
                LEFT JOIN ipa.ifo_index_code i
                ON mi.floor_code = i.index_code
                AND i.locale = 'VN'
                WHERE mi.floor_code  IN ('VNINDEX','VN30','VNXALL','VNDIAMOND','VN100')
                )
            LOOP
                INSERT INTO ipa.ifo_next_day_price(sec_code,sec_type,exchange_code,trans_date,basic_price,time)
                VALUES (r.index_code, r.index_type, r.exchange_code, ipa.get_next_working_date(p_date,1), r.basic_price, '00:00:00');
                COMMIT;
            END LOOP;
        ELSIF p_source = 'IPA' THEN
            FOR r IN (
                SELECT mi.index_code, i.index_type, i.exchange_code, mi.close_price AS basic_price
                FROM ifo_index_vn_trade mi
                LEFT JOIN ipa.ifo_index_code i
                ON mi.index_code = i.index_code
                AND i.locale = 'VN'
                WHERE mi.index_code  IN ('VNINDEX','VN30','VNXALL','VNDIAMOND','VN100')
                AND mi.trans_date = p_date
                )
            LOOP
                INSERT INTO ipa.ifo_next_day_price(sec_code,sec_type,exchange_code,trans_date,basic_price,time)
                VALUES (r.index_code, r.index_type, r.exchange_code, ipa.get_next_working_date(p_date,1), r.basic_price, '00:00:00');
                COMMIT;
            END LOOP;
        END IF;
        ipa.write_log('calc_price_pkg','update_next_day_price_index '||p_date,SYSDATE,SYSDATE,NULL, NULL, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','update_next_day_price_index '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    PROCEDURE update_trade_hist(p_date DATE) AS
    BEGIN
        ipa.write_log('calc_price_pkg','update_trade_hist '||ipa.get_working_date(p_date,1),SYSDATE,NULL,NULL, NULL, 'START');
        DELETE ipa.latest_trade_hist;
        COMMIT;
        FOR r IN (
            SELECT p.sec_code, p.exchange_code, MAX(p.trans_date) AS trans_date
            --, SUM(volume) AS hist_volume
            , SUM(p.volume+NVL(pt.volume,0)) AS hist_volume
            FROM ifo_sec_price p, ifo_sec_put_through pt
            WHERE p.sec_code = pt.sec_code (+)
            AND p.trans_date = pt.trading_date (+)
            AND p.trans_date >= ipa.get_working_date(p_date,25)
            AND p.trans_date <= ipa.get_working_date(p_date,1)
            GROUP BY p.sec_code, p.exchange_code)
        LOOP
            INSERT INTO ipa.latest_trade_hist(sec_code,exchange_code,trans_date,hist_volume) VALUES (r.sec_code,r.exchange_code,r.trans_date,r.hist_volume);
            COMMIT;
        END LOOP;
        ipa.write_log('calc_price_pkg','done update_trade_hist '||ipa.get_working_date(TRUNC(SYSDATE),1),SYSDATE,NULL,NULL, NULL, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','update_trade_hist '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    PROCEDURE insert_estimated_adjust (p_date DATE, p_source VARCHAR2) AS
        CURSOR c_rights IS
            SELECT DISTINCT c.sec_code, r.rights_date
            FROM ifo_sec_investor_rights r, ifo_sec_code c
            WHERE c.sec_id = r.sec_id
            AND r.rights_date = ipa.get_next_working_date(p_date,1)
            AND r.rights_type IN ('DIVIDEND','STOCKDIV','KINDDIV','ISSUE')
            AND NVL(r.status,'ACTIVE') NOT IN ('CANCEL','DELAY');

    BEGIN
        ipa.write_log('calc_price_pkg','insert_estimated_adjust for next day of '||p_date,SYSDATE, NULL, NULL, NULL, 'START');
        FOR v_rights IN c_rights LOOP
            DELETE FROM ipa.ifo_sec_price_adjust_log WHERE sec_Code = v_rights.sec_code AND adjust_date = v_rights.rights_date;
            INSERT INTO ifo_sec_price_adjust_log (adjust_date, sec_code, estimated_ratio, temp_dividend, temp_div_ratio
                , temp_stockdiv, temp_kinddiv, temp_issue, temp_issue_price, temp_issue_flag, created_date, created_by)
            SELECT
                v_rights.rights_date
                , v_rights.sec_code
                , a.ratio
                , a.dividend
                , a.div_ratio
                , a.stockdiv
                , a.kinddiv
                , a.issue
                , a.issue_price
                , a.issue_flag
                , SYSDATE
                , 'COMPUTER'
            FROM ipa.t_adjust_price(v_rights.sec_code,p_date,p_source) a;
            COMMIT;
            ipa.write_log('calc_price_pkg'
            ,'insert_estimated_adjust_daily '|| v_rights.sec_code ||' for '||v_rights.rights_date,SYSDATE,SYSDATE, NULL, NULL, 'SUCCESS');
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','insert_estimated_adjust for next day of '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    PROCEDURE insert_final_adjust_ratio (p_code VARCHAR2, p_date DATE, p_source VARCHAR2) AS
        CURSOR c_rights IS
            SELECT DISTINCT c.sec_code, r.rights_date
                , a.ratio
                , a.dividend
                , a.div_ratio
                , a.stockdiv
                , a.kinddiv
                , a.issue
                , a.issue_price
                , a.issue_flag
            FROM ifo_sec_investor_rights r, ifo_sec_code c
                , ipa.t_adjust_price(c.sec_code,p_date,p_source) a
            WHERE c.sec_id = r.sec_id
            AND c.sec_code = p_code
            AND r.rights_date = ipa.get_next_working_date(p_date,1)
            AND r.rights_type IN ('DIVIDEND','STOCKDIV','KINDDIV','ISSUE')
            AND NVL(r.status,'ACTIVE') NOT IN ('CANCEL','DELAY');

    v_rights       c_rights%rowtype;

    BEGIN
        OPEN c_rights;
        FETCH c_rights INTO v_rights;
        IF c_rights%FOUND THEN
            MERGE INTO ipa.ifo_sec_price_adjust_log a
            USING (SELECT v_rights.sec_code code FROM dual) b
            ON (a.sec_code = b.code AND a.adjust_date = v_rights.rights_date)
            WHEN MATCHED THEN
                UPDATE SET
                    adjust_ratio = v_rights.ratio
                    , dividend = v_rights.dividend
                    , div_ratio = v_rights.div_ratio
                    , stockdiv = v_rights.stockdiv
                    , kinddiv = v_rights.kinddiv
                    , issue = v_rights.issue
                    , issue_price = v_rights.issue_price
                    , issue_flag = v_rights.issue_flag
                    , modified_by = 'COMPUTER'
                    , modified_date = sysdate
                WHERE adjust_date = v_rights.rights_date
                AND sec_code = v_rights.sec_code
            WHEN NOT MATCHED THEN
                INSERT (adjust_date, sec_code, estimated_ratio, temp_dividend, temp_div_ratio
                    , temp_stockdiv, temp_kinddiv, temp_issue, temp_issue_price, temp_issue_flag, created_date, created_by
                    , adjust_ratio, dividend, div_ratio, stockdiv, kinddiv, issue, issue_price, issue_flag)
                VALUES (
                    v_rights.rights_date
                    , v_rights.sec_code
                    , 1
                    , 0
                    , 0
                    , 0
                    , 0
                    , 0
                    , 0
                    , 'N'
                    , SYSDATE
                    , 'COMPUTER'
                    , v_rights.ratio
                    , v_rights.dividend
                    , v_rights.div_ratio
                    , v_rights.stockdiv
                    , v_rights.kinddiv
                    , v_rights.issue
                    , v_rights.issue_price
                    , v_rights.issue_flag);
                COMMIT;
            ipa.write_log('calc_price_pkg','insert_final_adjust_ratio '|| v_rights.sec_code ||' for '||v_rights.rights_date,SYSDATE,SYSDATE, NULL, NULL, 'SUCCESS');
        END IF;
        CLOSE c_rights;
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','insert_final_adjust_ratio '|| v_rights.sec_code ||' for '||v_rights.rights_date||' fail.',SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    PROCEDURE adjust_sec_price_by_code(p_code VARCHAR2) AS
        CURSOR c_adjust IS
            SELECT sec_code,period_date,adjust_date,adjust_ratio
            FROM(
                SELECT sec_code,adjust_date,adjust_ratio,LAG (adjust_date,1) OVER (PARTITION BY sec_code ORDER BY adjust_date ASC) AS period_date
                FROM (
                    SELECT sc.sec_code, MIN(i.effective_date) adjust_date, 1 AS adjust_ratio
                    FROM ifo_sec_code sc
                       , ifo_company_ipo_status i
                    WHERE sc.company_id = i.company_id
                      AND i.ipo_status_type IN ('IPO3.6','IPO4.5','IPO5.5')
                      AND locale = 'VN'
                    GROUP BY sc.sec_code
                    UNION
                    SELECT sec_code, adjust_date, adjust_ratio
                    FROM ifo_sec_price_adjust_log
                    )
                )
            WHERE period_date IS NOT NULL
            AND sec_code = p_code
            ORDER BY sec_code,adjust_date DESC;

        v_adjust_value NUMBER;

    BEGIN
        ipa.write_log('calc_price_pkg','adjust_sec_price_by_code:'|| p_code,SYSDATE);
        v_adjust_value := 1;
        FOR v_adjust IN c_adjust LOOP
            v_adjust_value := v_adjust_value * v_adjust.adjust_ratio;
            UPDATE ifo_sec_price
            SET ad_close_price = close_price * v_adjust_value,
                ad_open_price = open_price * v_adjust_value,
                ad_high_price = high_price * v_adjust_value,
                ad_low_price = low_price * v_adjust_value,
                ad_average_price = average_price * v_adjust_value,
                modified_by = 'COMPUTER',
                modified_date = sysdate
            WHERE sec_code = v_adjust.sec_code
            AND trans_date >= v_adjust.period_date
            AND trans_date < v_adjust.adjust_date;
            COMMIT;
        END LOOP;
        ipa.write_log('calc_price_pkg','adjust_sec_price_by_code '|| p_code,SYSDATE,SYSDATE, NULL, p_code, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_adjust_pkg','adjust_sec_price_by_code:'|| p_code,NULL,SYSDATE,SUBSTR(SQLERRM, 1, 1000),p_code,'FAIL');
    END;

    PROCEDURE adjust_intra_price_by_code(p_code VARCHAR2) AS
        CURSOR c_adjust IS
            SELECT sec_code,period_date,adjust_date,adjust_ratio
            FROM(
                SELECT sec_code,adjust_date,adjust_ratio,LAG (adjust_date,1) OVER (PARTITION BY sec_code ORDER BY adjust_date ASC) AS period_date
                FROM (
                    SELECT sc.sec_code, MIN(i.effective_date) adjust_date, 1 AS adjust_ratio
                    FROM ifo_sec_code sc
                       , ifo_company_ipo_status i
                    WHERE sc.company_id = i.company_id
                      AND i.ipo_status_type IN ('IPO3.6','IPO4.5','IPO5.5')
                      AND locale = 'VN'
                    GROUP BY sc.sec_code
                    UNION
                    SELECT sec_code, adjust_date, adjust_ratio
                    FROM ifo_sec_price_adjust_log
                    )
                )
            WHERE period_date IS NOT NULL
            AND sec_code = p_code
            ORDER BY sec_code,adjust_date DESC;

        v_adjust_value NUMBER;
    BEGIN
        ipa.write_log('calc_adjust_pkg','adjust_intra_price_by_code:'|| p_code,SYSDATE);
        v_adjust_value := 1;
        FOR v_adjust IN c_adjust LOOP
            v_adjust_value := v_adjust_value * v_adjust.adjust_ratio;

            UPDATE ifo_sec_intra_history
            SET ad_match_price = match_price * v_adjust_value
            WHERE sec_code = v_adjust.sec_code
            AND trading_date >= v_adjust.period_date
            AND trading_date < v_adjust.adjust_date;
            COMMIT;

            UPDATE ifo_trading_summary
            SET ad_match_price = match_price * v_adjust_value
            WHERE sec_code = v_adjust.sec_code
            AND trading_date >= v_adjust.period_date
            AND trading_date < v_adjust.adjust_date;
            COMMIT;
        END LOOP;
        ipa.write_log('calc_adjust_pkg','adjust_intra_price_by_code:'|| p_code,SYSDATE,SYSDATE, NULL, p_code, 'SUCCESS');

    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_adjust_pkg','adjust_intra_price_by_code:'|| p_code,NULL,SYSDATE,SUBSTR(SQLERRM, 1, 1000),p_code,'FAIL');
    END;

    PROCEDURE update_next_day_price_stock(p_date DATE, p_source VARCHAR2) AS
        CURSOR c_update_list(p_next_date DATE) IS
            SELECT sc.sec_code, sc.sec_type, sc.exchange_code
                    ,ipa.get_krxmarketid(sc.sec_code,sc.sec_type,sc.exchange_code) market_id
                    ,get_tradestatus(sc.sec_code,p_next_date) krx_trade_status
                    ,get_santionstatus(sc.sec_code,p_next_date) krx_santion_status
            FROM ifo_sec_code sc, ifo_company_ipo_status ipo
            WHERE sc.company_id = ipo.company_id
            AND sc.sec_type IN ('STOCK','IFC','ETF')
            AND sc.exchange_code IN ('HOSTC','HNX','UPCOM')
            AND ipo.ipo_status_type IN ('IPO3.6','IPO4.5','IPO5.5')
            AND ipo.effective_date <= p_next_date
            AND NVL(ipo.expiration_date,'31-Dec-9999') >= p_next_date;

        v_next_day              DATE;
        v_basic_price           NUMBER;
        v_ceiling_price         NUMBER;
        v_floor_price           NUMBER;
        v_trading_status        VARCHAR2(10);
        v_round_status          VARCHAR2(10);
        v_odd_status            VARCHAR2(10);
        v_krx_halt_status       VARCHAR2(10);
        v_krx_admin_status      VARCHAR2(10);

    BEGIN
        ipa.write_log('calc_price_pkg','update_next_day_price_stock '||p_date,SYSDATE,NULL,NULL, NULL, 'START');
        update_trade_hist(p_date);
        v_next_day := ipa.get_next_working_date(p_date,1);
        FOR r IN c_update_list(v_next_day) LOOP
            v_krx_admin_status := 'NRM';
            v_krx_halt_status := (CASE WHEN r.krx_santion_status = 'SUS' THEN 'Y' ELSE 'N' END);
            v_trading_status := ipa.raw_roundlotstatus('N',r.krx_trade_status,r.krx_santion_status);
            v_round_status := ipa.get_roundlotstatus('N',r.krx_trade_status,r.krx_santion_status);
            v_odd_status := ipa.get_oddlotstatus('N',r.krx_trade_status,r.krx_santion_status);
            v_basic_price := NVL(get_next_day_round_basic(r.sec_code,p_date,p_source),0);
            v_ceiling_price := NVL(get_next_day_ceiling(r.sec_code,p_date,p_source),0);
            v_floor_price := NVL(get_next_day_floor_price(r.sec_code,p_date,p_source),0);

            IF v_basic_price > 0 THEN
                MERGE INTO ipa.ifo_next_day_price a
                USING (SELECT r.sec_code code FROM dual) b
                ON (a.sec_code = b.code)
                WHEN MATCHED THEN
                    UPDATE SET SEC_TYPE = r.sec_type
                        , EXCHANGE_CODE = r.exchange_code
                        , TRANS_DATE = v_next_day
                        , BASIC_PRICE = v_basic_price
                        , CEILING_PRICE = v_ceiling_price
                        , FLOOR_PRICE = v_floor_price
                        , TRADING_STATUS = v_trading_status
                        , TIME = '00:00:00'
                        , ROUND_LOT_STATUS = v_round_status
                        , ODD_LOT_STATUS = v_odd_status
                        , KRX_HALT_STATUS = v_krx_halt_status
                        , KRX_ADMIN_STATUS = v_krx_admin_status
                        , KRX_TRADE_STATUS = r.krx_trade_status
                        , KRX_SANTION_STATUS = r.krx_santion_status
                        , BUYIN_STATUS = 'N'
                    WHERE SEC_CODE = r.sec_code
                WHEN NOT MATCHED THEN
                INSERT (SEC_CODE
                        , SEC_TYPE
                        , EXCHANGE_CODE
                        , TRANS_DATE
                        , BASIC_PRICE
                        , CEILING_PRICE
                        , FLOOR_PRICE
                        , TRADING_STATUS
                        , TIME
                        , ROUND_LOT_STATUS
                        , ODD_LOT_STATUS
                        , KRX_HALT_STATUS
                        , KRX_ADMIN_STATUS
                        , KRX_TRADE_STATUS
                        , KRX_SANTION_STATUS
                        , BUYIN_STATUS)
                    VALUES (
                        r.sec_code
                        , r.sec_type
                        , r.exchange_code
                        , v_next_day
                        , v_basic_price
                        , v_ceiling_price
                        , v_floor_price
                        , v_trading_status
                        , '00:00:00'
                        , v_round_status
                        , v_odd_status
                        , v_krx_halt_status
                        , v_krx_admin_status
                        , r.krx_trade_status
                        , r.krx_santion_status
                        , 'N');
                    COMMIT;
            END IF;
        END LOOP;
        ipa.write_log('calc_price_pkg','update_next_day_price_stock '||p_date,SYSDATE,NULL,NULL, NULL, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','update_next_day_price_stock '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    FUNCTION get_warrant_ceiling(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) RETURN NUMBER AS
        CURSOR c_warrant (p_basic_price NUMBER) IS
            SELECT
                dc.derivative_code      AS sec_code,
                dc.underlying_asset     AS underlying,
                1/ substr(sp.exercise_ratio, 1, length(sp.exercise_ratio) - 2) AS exercise_ratio,
                ndp.ceiling_price AS underlying_ceiling_price,
                ndp.basic_price AS underlying_basic_price,
                (ndp.ceiling_price-ndp.basic_price)*(1/substr(sp.exercise_ratio, 1, length(sp.exercise_ratio) - 2))+p_basic_price AS ceiling_price
            FROM
                ifo_derivative_code           dc,
                ifo_derivative_strike_price   sp,
                ipa.ifo_next_day_price            ndp
            WHERE dc.derivative_code = p_code
                AND dc.derivative_code = sp.derivative_code
                AND dc.underlying_asset = ndp.sec_code
                AND sp.effective_date <= p_date
                AND NVL(sp.expiry_date, '31-Dec-9999') >= p_date;

        v_basic_price   NUMBER;
        v_result        NUMBER;
        v_warrant       c_warrant%rowtype;

    BEGIN
        v_basic_price := get_next_day_raw_basic(p_code,p_date,p_source);
        OPEN c_warrant(v_basic_price);
        FETCH c_warrant INTO v_warrant;
        IF c_warrant%FOUND THEN
            v_result := FLOOR(v_warrant.ceiling_price/10)*10;
        ELSE
            v_result := 0;
        END IF;
        CLOSE c_warrant;

        IF v_result = v_basic_price THEN
            v_result := v_basic_price + 10;
        END IF;
        RETURN ROUND(v_result,0);
    END;

    FUNCTION get_warrant_floor_price(p_code VARCHAR2, p_date DATE, p_source VARCHAR2) RETURN NUMBER AS
        CURSOR c_warrant(p_basic_price NUMBER) IS
            SELECT
                dc.derivative_code      AS sec_code,
                dc.underlying_asset     AS underlying,
                1/ substr(sp.exercise_ratio, 1, length(sp.exercise_ratio) - 2) AS exercise_ratio,
                ndp.floor_price AS underlying_floor_price,
                ndp.basic_price AS underlying_basic_price,
                p_basic_price-(ndp.basic_price-ndp.floor_price)*(1/substr(sp.exercise_ratio, 1, length(sp.exercise_ratio) - 2)) AS floor_price
            FROM
                ifo_derivative_code           dc,
                ifo_derivative_strike_price   sp,
                ipa.ifo_next_day_price            ndp
            WHERE dc.derivative_code = p_code
                AND dc.derivative_code = sp.derivative_code
                AND dc.underlying_asset = ndp.sec_code
                AND sp.effective_date <= p_date
                AND NVL(sp.expiry_date, '31-Dec-9999') >= p_date;

        v_basic_price   NUMBER;
        v_result        NUMBER;
        v_warrant       c_warrant%rowtype;

    BEGIN
        v_basic_price := get_next_day_raw_basic(p_code,p_date,p_source);
        OPEN c_warrant(v_basic_price);
        FETCH c_warrant INTO v_warrant;
        IF c_warrant%FOUND THEN
            v_result := CEIL(v_warrant.floor_price/10)*10;
        ELSE
            v_result := 10;
        END IF;
        CLOSE c_warrant;

        IF v_result <= 0 THEN
            v_result := 10;
        ELSIF v_result = v_basic_price AND v_basic_price >= 20 THEN
            v_result := v_basic_price - 10;
        ELSE
            v_result := v_result;
        END IF;
        RETURN ROUND(v_result,0);
    END;

    PROCEDURE update_next_day_price_warrant(p_date DATE, p_source VARCHAR2) AS
        CURSOR c_update_list(p_next_date DATE) IS
            SELECT derivative_code AS sec_code, derivative_type AS sec_type, exchange_code
            ,ipa.get_krxmarketid(derivative_code,derivative_type,exchange_code) market_id
            ,get_tradestatus(underlying_asset,p_next_date) krx_trade_status
            ,get_santionstatus(underlying_asset,p_next_date) krx_santion_status
            FROM ifo_derivative_code
            WHERE derivative_type = 'CW'
            AND first_trading_date <= p_next_date
            AND last_trading_date > p_next_date;

        v_next_day              DATE;
        v_basic_price           NUMBER;
        v_ceiling_price         NUMBER;
        v_floor_price           NUMBER;
        v_trading_status        VARCHAR2(10);
        v_round_status          VARCHAR2(10);
        v_odd_status            VARCHAR2(10);
        v_krx_halt_status       VARCHAR2(10);
        v_krx_admin_status      VARCHAR2(10);
    BEGIN
        ipa.write_log('calc_price_pkg','update_next_day_price_warrant '||p_date,SYSDATE,NULL,NULL, NULL, 'START');
        v_next_day := ipa.get_next_working_date(p_date,1);
        FOR r IN c_update_list(v_next_day) LOOP
            v_krx_admin_status := 'NRM';
            v_krx_halt_status := (CASE WHEN r.krx_santion_status = 'SUS' THEN 'Y' ELSE 'N' END);
            v_trading_status := ipa.raw_roundlotstatus('N',r.krx_trade_status,r.krx_santion_status);
            v_round_status := ipa.get_roundlotstatus('N',r.krx_trade_status,r.krx_santion_status);
            v_odd_status := ipa.get_oddlotstatus('N',r.krx_trade_status,r.krx_santion_status);
            v_basic_price := NVL(get_next_day_raw_basic(r.sec_code,p_date,p_source),0);
            v_ceiling_price := NVL(get_warrant_ceiling(r.sec_code,p_date,p_source),0);
            v_floor_price := NVL(get_warrant_floor_price(r.sec_code,p_date,p_source),0);

            IF v_basic_price > 0 THEN
                MERGE INTO ipa.ifo_next_day_price a
                USING (SELECT r.sec_code code FROM dual) b
                ON (a.sec_code = b.code)
                WHEN MATCHED THEN
                    UPDATE  SET SEC_TYPE = r.sec_type
                        , EXCHANGE_CODE = r.exchange_code
                        , TRANS_DATE = v_next_day
                        , BASIC_PRICE = v_basic_price
                        , CEILING_PRICE = v_ceiling_price
                        , FLOOR_PRICE = v_floor_price
                        , TRADING_STATUS = v_trading_status
                        , TIME = '00:00:00'
                        , ROUND_LOT_STATUS = v_round_status
                        , ODD_LOT_STATUS = v_odd_status
                        , KRX_HALT_STATUS = v_krx_halt_status
                        , KRX_ADMIN_STATUS = v_krx_admin_status
                        , KRX_TRADE_STATUS = r.krx_trade_status
                        , KRX_SANTION_STATUS = r.krx_santion_status
                        , BUYIN_STATUS = 'N'
                    WHERE SEC_CODE = r.sec_code
                WHEN NOT MATCHED THEN
                    INSERT (SEC_CODE
                        , SEC_TYPE
                        , EXCHANGE_CODE
                        , TRANS_DATE
                        , BASIC_PRICE
                        , CEILING_PRICE
                        , FLOOR_PRICE
                        , TRADING_STATUS
                        , TIME
                        , ROUND_LOT_STATUS
                        , ODD_LOT_STATUS
                        , KRX_HALT_STATUS
                        , KRX_ADMIN_STATUS
                        , KRX_TRADE_STATUS
                        , KRX_SANTION_STATUS
                        , BUYIN_STATUS)
                    VALUES (
                        r.sec_code
                        , r.sec_type
                        , r.exchange_code
                        , v_next_day
                        , v_basic_price
                        , v_ceiling_price
                        , v_floor_price
                        , v_trading_status
                        , '00:00:00'
                        , v_round_status
                        , v_odd_status
                        , v_krx_halt_status
                        , v_krx_admin_status
                        , r.krx_trade_status
                        , r.krx_santion_status
                        , 'N');
                    COMMIT;
            END IF;
        END LOOP;
        ipa.write_log('calc_price_pkg','update_next_day_price_warrant '||p_date,SYSDATE,SYSDATE,NULL, NULL, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','update_next_day_price_warrant '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    PROCEDURE update_settlement_price_future(p_date DATE) AS
        CURSOR c_update_list IS
            SELECT symbol, price_dsp
            FROM ipa.fdsinstruments
            --WHERE trunc(last_change) = p_date
            ORDER BY symbol;
    BEGIN
        ipa.write_log('calc_price_pkg','update_settlement_price_future '||p_date,SYSDATE,NULL,NULL, NULL, 'START');
        FOR r IN c_update_list LOOP
            UPDATE ipa.ifo_derivative_trade
                SET settlement_price = r.price_dsp
                , modified_date = sysdate
                , modified_by = 'COMPUTER'
            WHERE derivative_code = r.symbol
            AND trans_date = p_date;
            COMMIT;
        END LOOP;
        ipa.write_log('calc_price_pkg','update_settlement_price_future '||p_date,SYSDATE,SYSDATE,NULL, NULL, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','update_settlement_price_future '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    PROCEDURE update_next_day_price_future(p_date DATE) AS
        CURSOR c_update_list(p_next_date DATE) IS
            SELECT derivative_code AS sec_code, derivative_type AS sec_type, exchange_code
            ,ipa.get_krxmarketid(derivative_code,derivative_type,exchange_code) market_id
            ,pup.price_range
            ,pup.price_unit
            ,DECODE(dc.underlying_type,'INDEX',10,1) AS round_type
            ,pup.round_lot_status
            ,dc.first_trading_date
            ,dc.issue_price
            FROM ipa.ifo_derivative_code dc,
                (
                    SELECT * FROM ipa.ifo_price_update_parameter
                    WHERE effective_date <= p_next_date
                    AND NVL(expiration_date,'31-Dec-9999') >= p_next_date
                ) pup
            WHERE dc.derivative_code = pup.code (+)
            AND dc.derivative_type = 'FU'
            AND dc.first_trading_date <= p_next_date
            AND dc.last_trading_date >= p_next_date;

        v_next_day              DATE;
        v_basic_price           NUMBER;
        v_ceiling_price         NUMBER;
        v_floor_price           NUMBER;
        v_trading_status        VARCHAR2(10);
        v_round_status          VARCHAR2(10);
        v_krx_halt_status       VARCHAR2(1);
    BEGIN
        ipa.write_log('calc_price_pkg','update_next_day_price_future '||p_date,SYSDATE,NULL,NULL, NULL, 'START');
        v_next_day := ipa.get_next_working_date(p_date,1);
        FOR r IN c_update_list(v_next_day) LOOP
            v_krx_halt_status := (CASE WHEN r.round_lot_status = 'N' THEN 'N' WHEN r.round_lot_status = 'H' THEN 'H' END);
            v_trading_status := ipa.raw_roundlotstatus(r.round_lot_status,'NRM','NRM');
            v_round_status := ipa.get_roundlotstatus(r.round_lot_status,'NRM','NRM');
            ---
            IF r.first_trading_date = v_next_day AND NVL(r.issue_price,0) > 0 THEN
                v_basic_price := r.issue_price;
            ELSE
                v_basic_price := NVL(get_next_day_raw_basic(r.sec_code,p_date,'IPA'),0); --mặc định lấy IPA vì phải lấy giá thanh toán
            END IF;

            v_ceiling_price := FLOOR(v_basic_price*(1+r.price_range)*r.round_type)/r.round_type;
            IF v_ceiling_price = v_basic_price THEN
                v_ceiling_price := v_basic_price + r.price_unit;
            END IF;

            v_floor_price := CEIL(v_basic_price*(1-r.price_range)*r.round_type)/r.round_type;
            IF v_floor_price = v_basic_price AND v_basic_price - r.price_unit > 0 THEN
                v_floor_price := v_basic_price - r.price_unit;
            END IF;

            IF v_basic_price > 0 THEN
                MERGE INTO ipa.ifo_next_day_price a
                USING (SELECT r.sec_code code FROM dual) b
                ON (a.sec_code = b.code)
                WHEN MATCHED THEN
                     UPDATE SET SEC_TYPE = r.sec_type
                        , EXCHANGE_CODE = r.exchange_code
                        , TRANS_DATE = v_next_day
                        , BASIC_PRICE = v_basic_price
                        , CEILING_PRICE = v_ceiling_price
                        , FLOOR_PRICE = v_floor_price
                        , TRADING_STATUS = v_trading_status
                        , TIME = '00:00:00'
                        , ROUND_LOT_STATUS = v_round_status
                        , ODD_LOT_STATUS = 'N'
                        , KRX_HALT_STATUS = v_krx_halt_status
                        , KRX_ADMIN_STATUS = 'NRM'
                        , KRX_TRADE_STATUS = 'NRM'
                        , KRX_SANTION_STATUS = 'NRM'
                        , BUYIN_STATUS = 'N'
                        -- future khong co odd, admin, trade, santion va buyin stt nen set mac dinh
                    WHERE SEC_CODE = r.sec_code
                WHEN NOT MATCHED THEN
                    INSERT ( SEC_CODE
                        , SEC_TYPE
                        , EXCHANGE_CODE
                        , TRANS_DATE
                        , BASIC_PRICE
                        , CEILING_PRICE
                        , FLOOR_PRICE
                        , TRADING_STATUS
                        , TIME
                        , ROUND_LOT_STATUS
                        , ODD_LOT_STATUS
                        , KRX_HALT_STATUS
                        , KRX_ADMIN_STATUS
                        , KRX_TRADE_STATUS
                        , KRX_SANTION_STATUS
                        , BUYIN_STATUS)
                    VALUES (
                        r.sec_code
                        , r.sec_type
                        , r.exchange_code
                        , v_next_day
                        , v_basic_price
                        , v_ceiling_price
                        , v_floor_price
                        , v_trading_status
                        , '00:00:00'
                        , v_round_status
                        , 'N'
                        , v_krx_halt_status
                        , 'NRM'
                        , 'NRM'
                        , 'NRM'
                        , 'N');
                    COMMIT;
            END IF;
        END LOOP;

        -- cac ma FU da duoc update tam theo gia dong cua vao 3h15 va gio duoc update lai o prc future khi co gia thanh toan
        FOR r IN (
            SELECT dc.derivative_code AS sec_code, dc.derivative_type AS sec_type, dc.exchange_code, dc.last_trading_date AS effective_date
                , get_current_price(dc.derivative_code,p_date,'IPA') AS current_price -- mac dinh lay IPA vi phai lay gia thanh toan
            FROM ifo_derivative_code dc
            WHERE dc.derivative_type = 'FU'
            AND dc.last_trading_date = p_date --ngay giao dich cuoi cung
            )
        LOOP
            MERGE INTO ipa.ifo_next_day_price a
            USING (SELECT r.sec_code code FROM dual) b
            ON (a.sec_code = b.code)
            WHEN MATCHED THEN
                UPDATE SET SEC_TYPE = r.sec_type
                    , EXCHANGE_CODE = r.exchange_code
                    , TRANS_DATE = v_next_day
                    , BASIC_PRICE = r.current_price
                    , CEILING_PRICE = r.current_price
                    , FLOOR_PRICE = r.current_price
                    , TRADING_STATUS = 'S'
                    , TIME = '00:00:00'
                    , ROUND_LOT_STATUS = 'H'
                    , ODD_LOT_STATUS = 'H'
                    , KRX_HALT_STATUS = 'Y'
                    , KRX_ADMIN_STATUS = 'DLD'
                    , KRX_TRADE_STATUS = 'DLD'
                    , KRX_SANTION_STATUS = 'DLD'
                    , BUYIN_STATUS = 'N'
                WHERE SEC_CODE = r.sec_code
            WHEN NOT MATCHED THEN
            INSERT (SEC_CODE
                    , SEC_TYPE
                    , EXCHANGE_CODE
                    , TRANS_DATE
                    , BASIC_PRICE
                    , CEILING_PRICE
                    , FLOOR_PRICE
                    , TRADING_STATUS
                    , TIME
                    , ROUND_LOT_STATUS
                    , ODD_LOT_STATUS
                    , KRX_HALT_STATUS
                    , KRX_ADMIN_STATUS
                    , KRX_TRADE_STATUS
                    , KRX_SANTION_STATUS
                    , BUYIN_STATUS)
                VALUES (
                    r.sec_code
                    , r.sec_type
                    , r.exchange_code
                    , v_next_day
                    , r.current_price
                    , r.current_price
                    , r.current_price
                    , 'S'
                    , '00:00:00'
                    , 'H'
                    , 'H'
                    , 'Y'
                    , 'DLD'
                    , 'DLD'
                    , 'DLD'
                    , 'N');
                COMMIT;
        END LOOP;
        ipa.write_log('calc_price_pkg','update_delisting_future '||p_date,SYSDATE,SYSDATE,NULL, NULL, 'SUCCESS');
        ipa.write_log('calc_price_pkg','update_next_day_price_future '||p_date,SYSDATE,SYSDATE,NULL, NULL, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','update_next_day_price_future '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;

    PROCEDURE update_delisting_security(p_date DATE, p_source VARCHAR2) AS
        v_next_day              DATE;

    BEGIN
        ipa.write_log('calc_price_pkg','update_delisting_security '||p_date,SYSDATE,NULL,NULL, NULL, 'START');
        v_next_day := ipa.get_next_working_date(p_date,1);
        FOR r IN (
            SELECT sc.sec_code, sc.sec_type, sc.exchange_code, ipo.effective_date
                , get_current_price(sc.sec_code,p_date,p_source) AS current_price
            FROM ifo_company_ipo_status ipo, ifo_sec_code sc
            WHERE ipo.company_id = sc.company_id
            AND ipo.ipo_status_type IN ('IPO3.7','IPO4.7','IPO5.6') --ngay giao dich cuoi cung
            AND ipo.effective_date = p_date
            UNION ALL
            SELECT dc.derivative_code AS sec_code, dc.derivative_type AS sec_type, dc.exchange_code, dc.last_trading_date AS effective_date
                , get_current_price(dc.derivative_code,p_date,p_source) AS current_pricce
            FROM ifo_derivative_code dc
            WHERE dc.last_trading_date = p_date --ngay giao dich cuoi cung
            -- cac ma FU duoc update tam theo gia dong cua vao 3h15 va se duoc update lai o prc future khi co gia thanh toan
            UNION ALL
            SELECT sec_code, sec_type, exchange_code, last_trading_date AS effective_date
                , ipa.calc_price_pkg.get_current_price(sec_code,p_date,'QUOTE') AS current_pricce
            FROM ifo_sec_code
            WHERE sec_type = 'BOND'
            AND last_trading_date = p_date --ngay giao dich cuoi cung
            )
        LOOP
            MERGE INTO ipa.ifo_next_day_price a
            USING (SELECT r.sec_code code FROM dual) b
            ON (a.sec_code = b.code)
            WHEN MATCHED THEN
                UPDATE SET SEC_TYPE = r.sec_type
                    , EXCHANGE_CODE = r.exchange_code
                    , TRANS_DATE = v_next_day
                    , BASIC_PRICE = r.current_price
                    , CEILING_PRICE = r.current_price
                    , FLOOR_PRICE = r.current_price
                    , TRADING_STATUS = 'S'
                    , TIME = '00:00:00'
                    , ROUND_LOT_STATUS = 'H'
                    , ODD_LOT_STATUS = 'H'
                    , KRX_HALT_STATUS = 'Y'
                    , KRX_ADMIN_STATUS = 'DLD'
                    , KRX_TRADE_STATUS = 'DLD'
                    , KRX_SANTION_STATUS = 'DLD'
                    , BUYIN_STATUS = 'N'
                WHERE SEC_CODE = r.sec_code
            WHEN NOT MATCHED THEN
            INSERT (SEC_CODE
                    , SEC_TYPE
                    , EXCHANGE_CODE
                    , TRANS_DATE
                    , BASIC_PRICE
                    , CEILING_PRICE
                    , FLOOR_PRICE
                    , TRADING_STATUS
                    , TIME
                    , ROUND_LOT_STATUS
                    , ODD_LOT_STATUS
                    , KRX_HALT_STATUS
                    , KRX_ADMIN_STATUS
                    , KRX_TRADE_STATUS
                    , KRX_SANTION_STATUS
                    , BUYIN_STATUS)
                VALUES (
                    r.sec_code
                    , r.sec_type
                    , r.exchange_code
                    , v_next_day
                    , r.current_price
                    , r.current_price
                    , r.current_price
                    , 'S'
                    , '00:00:00'
                    , 'H'
                    , 'H'
                    , 'Y'
                    , 'DLD'
                    , 'DLD'
                    , 'DLD'
                    , 'N');
                COMMIT;
        END LOOP;
        ipa.write_log('calc_price_pkg','update_delisting_security '||p_date,SYSDATE,SYSDATE,NULL, NULL, 'SUCCESS');
    EXCEPTION WHEN OTHERS THEN
        ipa.write_log('calc_price_pkg','update_delisting_security '||p_date,SYSDATE,SYSDATE,SUBSTR(SQLERRM, 1, 1000),NULL, 'FAIL');
    END;
END;