\timing 
select * from (
 SELECT ds.host_rcpn, ds.device_id, ds.device_type, ds.timestamp_utc as last_update_utc,
    COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='BATTERY' limit 1), 0) as  storage,
    COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='PVLINK' limit 1), 0) as solar,
    ((now() - ds.timestamp_utc) > '3 hours')::int as offline,
    -- if any devices tied to an inverter have an error, then select 1
    COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
    FROM unnest( ARRAY[
 ('0001000719AD'),
 ('000100082FD6'),
 ('000100081FD2'),
 ('00010007280A'),
 ('0001000803A6'),
 ('000100071D1A'),
 ('00010007245D'),
 ('00010007304C'),
 ('0001000834F8'),
 ('0001000823F6'),
 ('000100071A0C'),
 ('00010008204A'),
 ('00010008200C'),
 ('000100080903'),
 ('000100072E10'),
 ('0001000810DB'),
 ('000100072CAD'),
 ('0001000706BE'),
 ('0001000813B5'),
 ('00010007365C'),
 ('000100070CCC'),
 ('000100071F3E'),
 ('0001000819E3'),
 ('00010008093F'),
 ('000100071E58'),
 ('000100070729'),
 ('000100070532'),
 ('000100072920'),
 ('0001000729F6'),
 ('000100072E01'),
 ('0001000719BB'),
 ('0001000835EF'),
 ('0001000728BD'),
 ('0001000724C0'),
 ('000100070658'),
 ('000100074110'),
 ('000100081042'),
 ('00010007143E'),
 ('0001000703AB'),
 ('000100072F86'),
 ('00010007280B'),
 ('000100072EFC'),
 ('000100080A4C'),
 ('000100070FF2'),
 ('0001000719F7'),
 ('000100081853'),
 ('00010008037B'),
 ('000100081107'),
 ('000100081E56'),
 ('000100081C34'),
 ('0001000700AD'),
 ('000100070732'),
 ('000100080284'),
 ('0001000728CC'),
 ('000100071B82'),
 ('000100081C2B'),
 ('00010007388A'),
 ('000100081B02'),
 ('000100080B75'),
 ('000100083658'),
 ('000100081472'),
 ('000100073000'),
 ('000100081781'),
 ('000100081006'),
 ('000100081355'),
 ('0001000809F2'),
 ('0001000709DF'),
 ('00010008125E'),
 ('00010007165C'),
 ('000100072497'),
 ('000100072F6B'),
 ('00010007008E'),
 ('000100081385'),
 ('00010008036D'),
 ('000100080AC9'),
 ('00010007110A'),
 ('000100081495'),
 ('000100071987'),
 ('000100071A90'),
 ('0001000828D6'),
 ('0001000813CC'),
 ('000100073A15'),
 ('00010007092E'),
 ('0001000710DA'),
 ('000100080F52'),
 ('000100071C8A'),
 ('0001000710E6'),
 ('0001000706B3'),
 ('0001000716AA'),
 ('0001000717CC'),
 ('000100072506'),
 ('000100071BF0'),
 ('0001000725C1'),
 ('000100083278'),
 ('00010008264F'),
 ('000100071E6A'),
 ('0001000718B1'),
 ('000100080C04'),
 ('000100081F17'),
 ('000100082639'),
 ('000100072A7E'),
 ('000100070235'),
 ('000100073092'),
 ('000100072EF5'),
 ('000100081173'),
 ('000100071BE3'),
 ('00010008162A'),
 ('000100070F1C'),
 ('000100070B0C'),
 ('000100081C5D'),
 ('0001000707BA'),
 ('000100072E78'),
 ('000100080620'),
 ('000100070C74'),
 ('000100073E43'),
 ('000100070270'),
 ('0001000811EB'),
 ('000100072EA4'),
 ('000100071B64'),
 ('00010008140B'),
 ('000100082C20'),
 ('000100081170'),
 ('0001000726BF'),
 ('000100071B47'),
 ('000100082F98'),
 ('000100073078'),
 ('000100081824'),
 ('000100072A0E'),
 ('00010008246C'),
 ('0001000805FB'),
 ('000100071DD8'),
 ('000100072947'),
 ('000100071561'),
 ('000100071C48'),
 ('000100070D71'),
 ('00010007101B'),
 ('000100071A07'),
 ('00010008188F'),
 ('000100070705'),
 ('000100070DFB'),
 ('000100073111'),
 ('000100070688'),
 ('0001000702D9'),
 ('000100070AEC'),
 ('000100082C9E'),
 ('00010007100B'),
 ('000100072A03'),
 ('000100070CA3'),
 ('000100071CC2'),
 ('000100072C59'),
 ('000100081F3A'),
 ('00010008255F'),
 ('000100070C7D'),
 ('00010007229B'),
 ('000100081552'),
 ('000100070F6C'),
 ('000100072855'),
 ('0001000710AB'),
 ('000100082134'),
 ('000100071D27'),
 ('0001000827BB'),
 ('00010008291E'),
 ('000100082731'),
 ('0001000703B3'),
 ('000100082C4A'),
 ('000100072652'),
 ('000100081057'),
 ('000100080C10'),
 ('000100082262'),
 ('000100072F92'),
 ('000100071AF5'),
 ('000100071B08'),
 ('00010007338D'),
 ('0001000839EF'),
 ('0001000725B4'),
 ('000100071E82'),
 ('0001000704B7'),
 ('000100080647'),
 ('000100073370'),
 ('000100072024'),
 ('00010008176A'),
 ('0001000812FE'),
 ('000100072645'),
 ('000100071AB5'),
 ('00010007306F'),
 ('00010008002F'),
 ('000100083004'),
 ('0001000715CC'),
 ('000100070ED2'),
 ('0001000715A7'),
 ('000100080311'),
 ('0001000815F8'),
 ('000100071ECF'),
 ('000100072751'),
 ('000100071756'),
 ('000100071A44'),
 ('000100072A8D'),
 ('00010007223A'),
 ('000100081147'),
 ('000100070E0D'),
 ('000100072299'),
 ('0001000825A3'),
 ('000100072229'),
 ('0001000726DC'),
 ('0001000829DC'),
 ('000100072231'),
 ('000100081EBB'),
 ('00010008307B'),
 ('0001000710D4'),
 ('0001000814B2'),
 ('0001000718C1'),
 ('0001000708C9'),
 ('0001000701DB'),
 ('00010007178E'),
 ('000100070A5C'),
 ('000100083628'),
 ('000100070B4D'),
 ('000100072C46'),
 ('0001000717E5'),
 ('000100072D61'),
 ('000100081B8F'),
 ('00010007254D'),
 ('0001000707A9'),
 ('000100071A99'),
 ('000100081FD4'),
 ('000100082291'),
 ('0001000719C2'),
 ('00010008071E'),
 ('0001000835E0'),
 ('000100072653'),
 ('000100081837'),
 ('0001000720E3'),
 ('000100072E7E'),
 ('000100083703'),
 ('000100070C8E'),
 ('000100081B93'),
 ('000100082EC9'),
 ('000100080C3C'),
 ('00010008032C'),
 ('000100082F66'),
 ('000100071238'),
 ('000100081C85'),
 ('000100072C0B'),
 ('0001000812FD'),
 ('000100072D6C'),
 ('000100073463'),
 ('00010008104A'),
 ('0001000706B5'),
 ('0001000730D4'),
 ('0001000827F0'),
 ('000100070EF6'),
 ('000100081917'),
 ('000100070511'),
 ('0001000712A8'),
 ('000100081615'),
 ('000100070DFC'),
 ('000100073345'),
 ('000100070BB2'),
 ('000100071D26'),
 ('000100070749'),
 ('0001000724F4'),
 ('000100071E47'),
 ('00010007129B'),
 ('00010007B817'),
 ('00010007292A'),
 ('0001000825E5'),
 ('000100071CCF'),
 ('00010008264D'),
 ('0001000701D5'),
 ('00010007258A'),
 ('000100071583'),
 ('000100072DEB'),
 ('00010008052F'),
 ('000100083191'),
 ('0001000721C7'),
 ('000100081CFE'),
 ('0001000701B8'),
 ('000100070B54'),
 ('000100073176'),
 ('00010008352B'),
 ('00010008353F'),
 ('000100080600'),
 ('00010008386C'),
 ('000100083604'),
 ('000100082319'),
 ('0001000805F3'),
 ('0001000818CF'),
 ('00010007095A'),
 ('000100082654'),
 ('0001000731D5'),
 ('000100070BD0'),
 ('000100082BF5'),
 ('000100070440'),
 ('0001000810A7'),
 ('000100081613'),
 ('000100080871'),
 ('000100071BEB'),
 ('0001000728F3'),
 ('00010008232A'),
 ('000100071D57'),
 ('000100072DF8'),
 ('000100070C6D'),
 ('000100071BD7'),
 ('0001000721E5'),
 ('000100072317'),
 ('0001000819EF'),
 ('000100070572'),
 ('00010008354F'),
 ('000100072FFB'),
 ('00010008269C'),
 ('0001000822AD'),
 ('0001000822FD'),
 ('0001000716DE'),
 ('000100080392'),
 ('000100080A23'),
 ('000100071BCB'),
 ('000100071081'),
 ('000100072E9B'),
 ('00010007288C'),
 ('000100070754'),
 ('0001000710ED'),
 ('00010008152A'),
 ('000100081383'),
 ('0001000836EE'),
 ('0001000819F5'),
 ('000100070587'),
 ('00010008156A'),
 ('000100080370'),
 ('0001000807CF'),
 ('00010008209B'),
 ('000100072731'),
 ('0001000836F2'),
 ('000100071E31'),
 ('00010008246A'),
 ('0001000821F9'),
 ('000100071D1F'),
 ('000100072349'),
 ('000100072357'),
 ('000100072918'),
 ('0001000825F7'),
 ('00010008228B'),
 ('000100080FC9'),
 ('00010008293C'),
 ('000100072788'),
 ('000100082D61'),
 ('0001000732C9'),
 ('0001000817AC'),
 ('0001000727EE'),
 ('000100080C08'),
 ('00010007340F'),
 ('000100081BC8'),
 ('000100081AB8'),
 ('000100072975'),
 ('000100072413'),
 ('000100070FF4'),
 ('000100071692'),
 ('000100080A97'),
 ('000100071816'),
 ('0001000714DC'),
 ('0001000810C6'),
 ('00010008232B'),
 ('00010007247D'),
 ('000100070304'),
 ('0001000724B2'),
 ('000100072164'),
 ('0001000701C0'),
 ('00010007070F'),
 ('00010008196D'),
 ('000100073200'),
 ('0001000829D5'),
 ('000100070BA4'),
 ('00010008055A'),
 ('00010008158F'),
 ('000100072ECB'),
 ('000100071BE4'),
 ('00010007192E'),
 ('0001000717D2'),
 ('000100071833'),
 ('00010008102A'),
 ('0001000707E5'),
 ('000100071F91'),
 ('000100071D9B'),
 ('000100071B3A'),
 ('0001000818B3'),
 ('0001000715D8'),
 ('000100081501'),
 ('000100081F3B'),
 ('00010007014A'),
 ('0001000828BF'),
 ('000100071A86'),
 ('0001000709F2'),
 ('000100081702'),
 ('000100071EE0'),
 ('000100072552'),
 ('000100081A16'),
 ('000100072D2D'),
 ('00010008078E'),
 ('000100080A62'),
 ('0001000810EA'),
 ('000100070E96'),
 ('00010007047A'),
 ('0001000708E7'),
 ('000100081DC2'),
 ('0001000725B6'),
 ('000100082A2D'),
 ('000100071A2B'),
 ('000100080728'),
 ('000100071675'),
 ('000100070F4A'),
 ('000100071295'),
 ('0001000809E5'),
 ('000100081AC7'),
 ('0001000714D0'),
 ('000100071433'),
 ('0001000829B9'),
 ('00010007161E'),
 ('000100081660'),
 ('000100072C17'),
 ('000100070E0F'),
 ('000100072F95'),
 ('0001000821E6'),
 ('00010008129C'),
 ('000100070338'),
 ('0001000812B4'),
 ('000100081567'),
 ('0001000821A1'),
 ('000100071029'),
 ('000100080C13'),
 ('000100071926'),
 ('0001000811CB'),
 ('000100080815'),
 ('000100081961'),
 ('000100083479'),
 ('000100072E6F'),
 ('000100082685'),
 ('000100082671'),
 ('0001000820C2'),
 ('000100081DDC'),
 ('000100071A6F'),
 ('00010007CEE4'),
 ('000100072E70'),
 ('0001000706E6'),
 ('000100071BC4'),
 ('000100071AC4'),
 ('0001000812B7'),
 ('000100071075'),
 ('000100081C3C'),
 ('00010007148B'),
 ('000100080F5C'),
 ('00010007072C'),
 ('00010008144C'),
 ('000100080C00'),
 ('00010007325D'),
 ('0001000730BB'),
 ('0001000720E6'),
 ('0001000714A4'),
 ('00010007206A'),
 ('0001000828C2'),
 ('0001000821D0'),
 ('000100070421'),
 ('000100081A74'),
 ('000100071EC7'),
 ('000100081252'),
 ('000100072621'),
 ('00010008111B'),
 ('000100080419'),
 ('0001000705DF'),
 ('000100080A13'),
 ('000100073361'),
 ('000100071DB3'),
 ('000100073417'),
 ('000100082094'),
 ('0001000814BA'),
 ('000100081A8D'),
 ('000100071271'),
 ('000100081204'),
 ('000100072976'),
 ('000100081FF9'),
 ('0001000812F2'),
 ('000100071F61'),
 ('00010007256B'),
 ('000100080EA2'),
 ('0001000711BD'),
 ('000100070F7B'),
 ('000100072E0E'),
 ('0001000720A8'),
 ('0001000709F8'),
 ('000100081A1E'),
 ('000100071766'),
 ('000100070E44'),
 ('000100071D8E'),
 ('00010007078D'),
 ('000100073DA2'),
 ('0001000801E0'),
 ('000100070D5B'),
 ('000100081C5B'),
 ('000100072756'),
 ('000100070D69'),
 ('0001000721CA'),
 ('000100070F13'),
 ('000100073059'),
 ('0001000703F7'),
 ('000100070612'),
 ('00010008162E'),
 ('000100081983'),
 ('0001000717F3'),
 ('000100071982'),
 ('000100072A2D'),
 ('0001000710CD'),
 ('0001000716D5'),
 ('00010007265B'),
 ('00010008387A'),
 ('0001000730EC'),
 ('000100073319'),
 ('000100070B73'),
 ('000100072ABD'),
 ('0001000836BD'),
 ('000100081578'),
 ('000100072BBA'),
 ('000100081F8C'),
 ('000100081429'),
 ('000100070E7E'),
 ('000100071F5D'),
 ('00010007292C'),
 ('00010008037E'),
 ('0001000815E0'),
 ('00010007254A'),
 ('000100071BE0'),
 ('000100072338'),
 ('00010007082B'),
 ('0001000746A4'),
 ('000100072176'),
 ('00010008194C'),
 ('000100081F44'),
 ('0001000811F9'),
 ('000100072FEE'),
 ('000100072C52'),
 ('00010007211F'),
 ('00010007265A'),
 ('000100080742'),
 ('0001000701AE'),
 ('00010007268F'),
 ('000100082DB9'),
 ('0001000830CE'),
 ('0001000817D2'),
 ('000100071ABE'),
 ('000100082521'),
 ('000100072F2F'),
 ('00010007176E'),
 ('000100070B03'),
 ('00010007343C'),
 ('000100072739'),
 ('000100071876'),
 ('00010008075B'),
 ('000100070A6A'),
 ('000100081CC4'),
 ('00010008152B'),
 ('0001000835F7'),
 ('000100071FD6'),
 ('000100081445'),
 ('0001000721AF'),
 ('000100082CBB'),
 ('000100071A64'),
 ('00010008209E'),
 ('0001000802B5'),
 ('0001000708CC'),
 ('000100080B4B'),
 ('000100071A38'),
 ('000100082590'),
 ('000100081FAD'),
 ('0001000733F3'),
 ('000100081740'),
 ('000100070D4F'),
 ('00010007162C'),
 ('0001000807C2'),
 ('000100081B19'),
 ('0001000706C6'),
 ('000100072461'),
 ('000100073D30'),
 ('000100070E29'),
 ('0001000816D7'),
 ('0001000708D1'),
 ('000100072EE8'),
 ('00010008073E'),
 ('000100081584'),
 ('00010007228E'),
 ('000100071E54'),
 ('00010008131F'),
 ('000100070FF9'),
 ('00010008025C'),
 ('000100082836'),
 ('00010007051B'),
 ('0001000730F3'),
 ('000100083589'),
 ('000100073304'),
 ('00010008112E'),
 ('0001000722DF'),
 ('0001000737E8'),
 ('000100073C52'),
 ('000100070B30'),
 ('0001000712B3'),
 ('000100071F6D'),
 ('000100071B23'),
 ('000100073446'),
 ('000100072159'),
 ('000100070CDC'),
 ('000100081041'),
 ('00010007209F'),
 ('0001000808D2'),
 ('00010007246E'),
 ('0001000817F2'),
 ('0001000726AC'),
 ('000100080B10'),
 ('000100080FA1'),
 ('0001000734EF'),
 ('00010007251C'),
 ('000100073047'),
 ('000100072EAE'),
 ('000100072FED'),
 ('00010008347A'),
 ('000100071915'),
 ('000100072A0A'),
 ('000100072010'),
 ('000100080B87'),
 ('0001000817CE'),
 ('000100082CE5'),
 ('0001000827C7'),
 ('000100072C74'),
 ('000100070B7E'),
 ('0001000809F5'),
 ('000100081039'),
 ('000100073104'),
 ('00010008025B'),
 ('0001000805DF'),
 ('000100081347'),
 ('000100080B20'),
 ('000100073101'),
 ('0001000718A2'),
 ('000100070735'),
 ('0001000820FB'),
 ('000100071AE4'),
 ('000100080F83'),
 ('00010008262F'),
 ('000100082A9B'),
 ('000100082CC1'),
 ('000100081ABB'),
 ('000100070EAC'),
 ('000100080BAE'),
 ('000100081929'),
 ('000100082193'),
 ('00010008238C'),
 ('0001000711B2'),
 ('000100071063'),
 ('0001000729A1'),
 ('0001000706D3'),
 ('00010007226D'),
 ('0001000803BF'),
 ('000100080974'),
 ('0001000803B2'),
 ('000100072F9D'),
 ('00010007278B'),
 ('00010008172B'),
 ('0001000730FB'),
 ('000100072200'),
 ('000100072F1E'),
 ('000100073737'),
 ('00010007035B'),
 ('000100071F3C'),
 ('0001000723EE'),
 ('0001000812A9'),
 ('0001000814AF'),
 ('00010007148E'),
 ('0001000836C0'),
 ('00010008233D'),
 ('000100072E6E'),
 ('000100071EFE'),
 ('00010007143F'),
 ('0001000808D8'),
 ('0001000808E8'),
 ('0001000716ED'),
 ('00010007198A'),
 ('00010008149A'),
 ('000100082B2A'),
 ('00010007105E'),
 ('000100071DE1'),
 ('000100071CE1'),
 ('000100080B00'),
 ('00010008096A'),
 ('000100071826'),
 ('000100071748'),
 ('000100081D07'),
 ('000100081720'),
 ('000100073157'),
 ('000100072294'),
 ('0001000811FA'),
 ('00010008278D'),
 ('000100070D88'),
 ('000100080AB0'),
 ('000100083603'),
 ('000100071F7C'),
 ('000100072E24'),
 ('0001000701E5'),
 ('000100070E4B'),
 ('00010007227D'),
 ('000100072922'),
 ('000100071B15'),
 ('0001000731F1'),
 ('000100070258'),
 ('0001000711E7'),
 ('000100070F34'),
 ('00010007158E'),
 ('000100070D05'),
 ('000100071ECE'),
 ('000100072A31'),
 ('00010007060A'),
 ('00010008126B'),
 ('00010007187F'),
 ('0001000717B1'),
 ('0001000724F5'),
 ('000100082B7A'),
 ('0001000817A3'),
 ('00010008174A'),
 ('00010008116B'),
 ('000100082AD5'),
 ('000100081C26'),
 ('0001000727D2'),
 ('000100082714'),
 ('00010007096C'),
 ('000100071C09'),
 ('000100070B04'),
 ('000100081E80'),
 ('000100080847'),
 ('00010008146A'),
 ('000100081C0B'),
 ('000100070B3A'),
 ('000100082293'),
 ('0001000835E8'),
 ('000100071129'),
 ('000100080ABC'),
 ('00010007102E'),
 ('000100072D3B'),
 ('000100082361'),
 ('0001000717F1'),
 ('0001000710F4'),
 ('000100081823'),
 ('0001000803AD'),
 ('000100070905'),
 ('000100070947'),
 ('00010008201B'),
 ('000100071B95'),
 ('00010008137F'),
 ('00010008305C'),
 ('00010007010A'),
 ('0001000807A4'),
 ('000100072C34'),
 ('00010007183E'),
 ('000100082D07'),
 ('000100070CA7'),
 ('0001000835C1'),
 ('000100082C5F'),
 ('00010007240C'),
 ('0001000702D0'),
 ('000100082502'),
 ('00010008036F'),
 ('0001000722A5'),
 ('000100073F48'),
 ('000100081D18'),
 ('0001000808BF'),
 ('000100080992'),
 ('000100071140'),
 ('000100071FC2'),
 ('000100080FE2'),
 ('000100081C8A'),
 ('000100082B1F'),
 ('0001000811F8'),
 ('000100071A2D'),
 ('00010007192F'),
 ('000100080F3D'),
 ('000100071C44'),
 ('000100070557'),
 ('0001000808F3'),
 ('0001000825B4'),
 ('0001000818C1'),
 ('0001000811FE'),
 ('000100070355'),
 ('000100071871'),
 ('0001000716B5'),
 ('000100072663'),
 ('000100081BF5'),
 ('000100082875'),
 ('0001000806A8'),
 ('00010007107F'),
 ('0001000709BD'),
 ('0001000819FB'),
 ('000100081B59'),
 ('0001000728C4'),
 ('000100070F85'),
 ('000100072EB9'),
 ('000100082F71'),
 ('000100071223'),
 ('000100072EDB'),
 ('000100071782'),
 ('000100072570'),
 ('000100072DC8'),
 ('000100080FD5'),
 ('00010007039A'),
 ('000100070008'),
 ('000100071811'),
 ('0001000723DA'),
 ('000100070FE5'),
 ('000100080F9D'),
 ('000100081D2C'),
 ('0001000818DF'),
 ('000100072DD2'),
 ('00010008152D'),
 ('0001000821FB'),
 ('00010007110C'),
 ('000100072A53'),
 ('000100072E1E'),
 ('000100081D14'),
 ('00010007241F'),
 ('0001000807CC'),
 ('00010008197B'),
 ('0001000720D1'),
 ('000100081FA8'),
 ('000100070F1E'),
 ('000100070695'),
 ('0001000707E2'),
 ('00010008086B'),
 ('000100072001'),
 ('00010007065F'),
 ('000100082959'),
 ('0001000727FD'),
 ('00010007014C'),
 ('00010007336C'),
 ('000100072DB6'),
 ('000100071230'),
 ('00010007462B'),
 ('00010007237E'),
 ('000100080874'),
 ('0001000712A1'),
 ('0001000813D9'),
 ('0001000730A5'),
 ('000100070AA8'),
 ('000100082E68'),
 ('000100071990'),
 ('0001000824EB'),
 ('000100070314'),
 ('000100072BC5'),
 ('0001000802BC'),
 ('0001000832CE'),
 ('000100082005'),
 ('0001000709D4'),
 ('000100081253'),
 ('000100070CC6'),
 ('0001000706AC'),
 ('000100073262'),
 ('000100082AEE'),
 ('000100070E4D'),
 ('0001000714FE'),
 ('000100081982'),
 ('000100072170'),
 ('000100070AD5'),
 ('000100081796'),
 ('0001000812C0'),
 ('000100071989'),
 ('000100071092'),
 ('000100072FC9'),
 ('000100072A2A'),
 ('00010008234C'),
 ('000100082101'),
 ('000100071CA7'),
 ('000100071444'),
 ('000100072C5A'),
 ('00010008364C'),
 ('00010007157D'),
 ('000100082108'),
 ('000100080624'),
 ('0001000706A4'),
 ('00010007058A'),
 ('000100081CC8'),
 ('000100071F69'),
 ('00010007184B'),
 ('0001000720BF'),
 ('000100072359'),
 ('000100081161'),
 ('000100071AD4'),
 ('000100081B3A'),
 ('000100072443'),
 ('000100081F95'),
 ('000100071D86'),
 ('0001000726DF'),
 ('000100072D50'),
 ('0001000832AE'),
 ('00010008184D'),
 ('000100072197'),
 ('0001000710AF'),
 ('000100081E41'),
 ('00010008199E'),
 ('000100082A68'),
 ('000100070BE9'),
 ('00010007319A'),
 ('00010007091F'),
 ('000100071030'),
 ('000100081944'),
 ('000100071C15'),
 ('0001000724EA'),
 ('000100070F4C'),
 ('0001000705FE'),
 ('0001000733AA'),
 ('0001000811F5'),
 ('000100081055'),
 ('0001000852BE'),
 ('000100070851'),
 ('00010008117F'),
 ('000100081521'),
 ('00010008127F'),
 ('00010008029F'),
 ('0001000702B4'),
 ('0001000720A0'),
 ('00010008253B'),
 ('0001000719FC'),
 ('000100071CD2'),
 ('0001000806D0'),
 ('000100070BA1'),
 ('000100081618'),
 ('0001000718DF'),
 ('000100081A6B'),
 ('00010007149D'),
 ('000100071513'),
 ('000100080B7B'),
 ('000100082828'),
 ('0001000823B3'),
 ('00010008184E'),
 ('0001000705BB'),
 ('0001000718CD'),
 ('0001000704A4'),
 ('0001000704B3'),
 ('0001000819AB'),
 ('0001000710A3'),
 ('000100071ED7'),
 ('00010007180E'),
 ('0001000819DB'),
 ('000100080BCE'),
 ('000100070A8F'),
 ('000100083509'),
 ('0001000715BE'),
 ('00010007188C'),
 ('000100071423'),
 ('00010007BB67'),
 ('000100081274'),
 ('000100082B11'),
 ('000100071F1E'),
 ('000100080348'),
 ('000100072166'),
 ('000100080B29'),
 ('0001000831F6'),
 ('000100073AE7'),
 ('000100082556'),
 ('000100081628'),
 ('000100071A04'),
 ('000100071783'),
 ('000100071BFA'),
 ('0001000829C9'),
 ('000100080580'),
 ('0001000734CA'),
 ('0001000808B8'),
 ('00010007313C'),
 ('000100071D46'),
 ('000100082F9B'),
 ('0001000823DC'),
 ('000100071DD6'),
 ('00010008006E'),
 ('0001000719E6'),
 ('0001000710D2'),
 ('00010007034C'),
 ('0001000838A1'),
 ('0001000823F5'),
 ('0001000815D5'),
 ('000100080579'),
 ('000100070144'),
 ('0001000726EC'),
 ('000100080C5E'),
 ('00010007122A'),
 ('00010008176E'),
 ('00010007267E'),
 ('000100072CEB'),
 ('00010008231A'),
 ('000100080C43'),
 ('000100070613'),
 ('000100081379'),
 ('000100070D2E'),
 ('000100072EDA'),
 ('000100071F3B'),
 ('000100080F2E'),
 ('000100070865'),
 ('000100082CA7'),
 ('0001000805CC'),
 ('000100071CF6'),
 ('0001000812AC')
  ]  ) as inverter(device_id) JOIN status.device_shadow ds on ( ds.device_id=inverter.device_id )
) p


CROSS JOIN LATERAL (
 select device_id, min_soc, avg_soc, max_soc, day,
  lag(day) over (order by day) yesterday 
  from status.legacy_status_daily    
  where day >= date_trunc('day',NOW())-INTERVAL '1 day'
  and device_id = p.device_id
  order by day desc
  limit 1
) t1


CROSS JOIN LATERAL (
 select device_id, min_soc, avg_soc, max_soc, month,
 lag(month) over(order by month) as last_month
  from status.legacy_status_monthly_materialized 
  where month >= date_trunc('month',NOW())-INTERVAL '1 months'
  and device_id = p.host_rcpn
  order by month desc
  limit 1
) t2


CROSS JOIN LATERAL (
  select device_id, total_whin, total_whout, avg_w, day 
  from status.battery_status_daily
  where day >= date_trunc('day',NOW())-INTERVAL '1 day'
  and device_id = p.device_id
  order by day desc
  limit 1
) t3 -- on TRUE

-- where p.error=1 
-- order by t3.total_whin DESC
limit 50 offset 220;
