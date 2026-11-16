*Mihaila Tudor 325CA*
# Protocol peste UDP
### Structuri si variabile folosite
	next_seq este numarul de secventa pe care il va primi urmatorul pachet
    nou care este trimis pe retea. Este incrementat  la fiecare pachet nou trimis.

	send_base este numarul de secventa al celui mai vechi pachet trimis, dar care nu a 
	fost inca confirmat prin ACK. Sliding window-ul inainteaza cand send_base creste.

	send_buf este bufferul in care clientul scrie datele.

	sent_packet_sizes retine dimensiunea fiecarui pachet trimis.E folosit
	pentru a gasi un pachet in send_buf

	duplicate_ack_count reprezinta numaral de cate ori primim consecutiv acelasi ACK.

	expected_seq este numarul de secventa al urmatorului pachet pe care          
    receptorul il asteapta in ordine.

  	recv_buf este un vector in care pun datele primite in ordine corecta.
	packets_buf este bufferul in care sunt tinute temporar pachetele care nu au
	ajuns in ordinea buna

	Valorile folosite pentru protocol_id si type sunt numerele pe care le-a purtat lamine yamal :)
### Functii
	init_receiver initializeaza serverul.Mai intai salveaza dimensiunea
	ferestrei in variabila globala server_recv_window si creeaza socketul UDP cu variabila globala main_sockfd

	wait4connect implementează partea de receptor a Three-Way Handshake.
	Mai intai asteapta intr-o bucla pachete pe socket-ul principal si verifica daca este de tip SYN_TYPE 
	si daca provine de la un client care nu este deja conectat.

	recv_data extrage datele primite in ordine corecta din recv_buf. Mai intai blocheaza mutex-ul conexiunii,
	iar daca recv_buf este gol, asteapta intr-o bucla pana cand apar date noi. Copiaza apoi numarul maxim
	de bytes posibil in bufferul primit ca parametru si ii sterge din recv_buf.

	receiver_handler ruleaza pe un thread separat si primeste incontinuu segmente de date de pe retea.
	Daca segmentul are protocol_id-ul si tipul corect, verifica numarul de secventa si daca e mai mic decat cel 
	asteptat, retrimite ACK; daca e in fereastra, il introduce in packets_buf, iar apoi ordoneaza si muta
	toate pachetele consecutive disponibile in recv_buf, incrementand expected_seq si trimitand un nou ACK.

	nit_sender calculeaza dimensiunea ferestrei de transmisie initiale in functie de viteza si delay-ul conexiunii

	setup_connection implementeaza partea de client pentru Three-Way Handshake. Trimite pachete SYN si asteapta
	raspunsul SYN-ACK cu portul dedicat alocat de receptor, apoi trimite ACK-ul de confirmare si initializeaza
	parametrii ferestrei glisante, socket-ul, timer-ul si mutex-ul.

	send_data este functia prin care se trimit  datele. Aceasta adauga datele noi in send_buf, iar daca
	fereastra glisanta permite, trimite pachetele imediat prin send_packet_by_seq, incrementand next_seq.

	sender_handler ruleaza pe un thread separat la transmitator. Asteapta ACK-uri sau timeout-uri.
	La primirea unui ACK nou mai mare decat send_base, avanseaza fereastra glisanta prin stergerea datelor confirmate din send_buf.
	La primirea a 3 ACK-uri duplicate consecutiv, retransmite pachetul pierdut.Daca apare timeout,
	retransmite primul pachet neconfirmat de la send_base. De asemenea, incearca mereu sa trimita noi pachete intrate in fereastra.

	send_packet_by_seq este o functie pe care  am facut-o pentru a trimite un pachet specificat de seq_num.
	Calculeaza offset-ul in send_buf pe baza dimensiunilor pachetelor trimise anterior si face header-ul pachetului
	si trimite segmentul pe retea.
	
