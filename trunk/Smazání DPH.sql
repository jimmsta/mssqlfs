
begin tran

declare @idsdok integer

DECLARE KurzorSDOK  CURSOR FOR 
     select idsdok
from sdok
inner join hdok on hdok.idhdok = sdok.idhdok
where cis_dok = 'POT1001893'

OPEN KurzorSDOK
	 WHILE 1 = 1 BEGIN
		  FETCH NEXT FROM KurzorSDOK INTO @idsdok
		  IF @@FETCH_STATUS < 0 BREAK 
		  exec spSDOK_UpdateRec @idsdok = @idsdok, @idsklad = 37
		  end
close KurzorSDOK
deallocate KurzorSDOK


commit tran