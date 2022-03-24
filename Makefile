#Makefile For Province:
#
#       CMCC           
#
###################################################################

SUBDIRS = sdl           \
          rdl3.0        \
          abmmdb3.0     \
          xcbalance     \
          tlsutil       \
          mapreduce3.0  \
		  owecycle      \
          queue         \
          deductfee     \
          public        \
          convertbill   \
          notifydb      \
          notifydeal     


ifeq ($(AI_VERSION), XZCMCC)
SUBDIRS += xzcmcc/queryserv
else
SUBDIRS += ngcmcc/queryserv
SUBDIRS += ngcmcc/procOweuserRule
SUBDIRS += ngcmcc/ngbase
SUBDIRS += ngcmcc/ngobject
SUBDIRS += ngcmcc/preywtrade
SUBDIRS += ngcmcc/percreditcal
SUBDIRS += ngcmcc/grpclasscal
SUBDIRS += ngcmcc/perglobalcreditcal
SUBDIRS += ngcmcc/gtm
endif

ifeq ($(AI_VERSION),NMCMCC)
SUBDIRS += nmcmcc/timingscan
SUBDIRS += nmcmcc/timingdeal
SUBDIRS += nmcmcc/luascript
endif

ifeq ($(AI_VERSION),JXCMCC)
SUBDIRS += jxcmcc
endif

SUBDIRS += timingdeal    \
           timingscan    \
           notifyfilter  \
           alterserv     \
           mdbscan       \
           assetupload   \
           notifyscan    \
           owecreate     \
           oneLittlePay  \
           abmprompt     \
           transasset    \
           lualoader     \
           creditcalculation \
           alterservdayacct
           

include $(OB_REL)/etc/NGMastermake

